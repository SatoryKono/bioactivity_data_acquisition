"""Shared runtime primitives for orchestrating ETL pipelines."""
from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

import pandas as pd

from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.definition import PipelineDefinition
from bioetl.core.pipeline.factory import StageFactory
from bioetl.core.pipeline.services import (
    ArtifactPlanner,
    MetadataService,
    OrchestrationService,
    QCService,
    RunMetadataBuilder,
    StagePlanExecutor,
    ValidationService,
    WriteService,
    default_artifact_planner_factory,
    default_metadata_service_factory,
    default_orchestration_service_factory,
    default_qc_service_factory,
)
from bioetl.core.pipeline.types import (
    PipelineBaseProtocol,
    RunArtifacts,
    RunResult,
    RunState,
    StageCommand,
    StageContext,
    StageDescriptor,
    StageExecutionOptions,
    WriteArtifacts,
)
from bioetl.qc.executor import QCMetricsExecutor
from bioetl.qc.plan import QCPlan


class PipelineRuntimeBase(ABC, PipelineBaseProtocol):
    """Common runtime for pipelines that orchestrate ETL stage plans."""

    deterministic_folder_prefix: str = "_"

    def __init__(
        self,
        config: Mapping[str, Any] | Any,
        pipeline_definition: PipelineDefinition | None = None,
        *,
        run_id: str | None = None,
        validator: Any | None = None,
        validation_service_factory: Callable[["PipelineRuntimeBase"], ValidationService]
        | None = None,
        write_service_factory: Callable[["PipelineRuntimeBase"], WriteService] | None = None,
        qc_executor_factory: Callable[[], QCMetricsExecutor] | None = None,
        qc_plan: QCPlan | None = None,
        qc_thresholds: Mapping[str, float] | None = None,
        qc_dry_run: bool | None = None,
        qc_enabled: bool | None = None,
        stage_plan_executor: StagePlanExecutor | None = None,
        artifact_planner: ArtifactPlanner | None = None,
        qc_service: QCService | None = None,
        metadata_service: MetadataService | None = None,
        run_metadata_builder: RunMetadataBuilder | None = None,
        orchestration_service_factory: Callable[["PipelineRuntimeBase"], OrchestrationService]
        | None = None,
        qc_service_factory: Callable[["PipelineRuntimeBase"], QCService] | None = None,
        metadata_service_factory: Callable[["PipelineRuntimeBase"], MetadataService] | None = None,
    ) -> None:
        self.config = config
        self.pipeline_definition = pipeline_definition or self._build_default_definition(config)
        self.run_id = run_id or uuid.uuid4().hex
        self.validator = validator
        self.pipeline_code = self._resolve_pipeline_code(config, self.pipeline_definition)
        materialization = getattr(config, "materialization", None)
        root = getattr(materialization, "root", None)
        self.output_root = Path(root) if root else Path.cwd()
        self.logs_directory = self.output_root.parent / "logs" / self.pipeline_code
        self.dry_run = False

        orchestration_factory = orchestration_service_factory or default_orchestration_service_factory(
            stage_plan_executor=stage_plan_executor, artifact_planner=artifact_planner
        )
        self.orchestration_service = orchestration_factory(self)
        self.stage_plan_executor = self.orchestration_service.stage_plan_executor
        self.artifact_planner = self.orchestration_service.artifact_planner

        qc_factory = qc_service_factory or default_qc_service_factory(
            qc_plan=qc_plan,
            executor_factory=qc_executor_factory,
            qc_thresholds=qc_thresholds,
            qc_dry_run=qc_dry_run,
            qc_enabled=qc_enabled,
        )
        self.qc_service = qc_service or qc_factory(self)

        metadata_factory = metadata_service_factory or default_metadata_service_factory(
            config, self.pipeline_code
        )
        self.metadata_service = metadata_service or metadata_factory(self)
        self.run_metadata_builder = run_metadata_builder or getattr(self.metadata_service, "builder", None)
        self._git_commit = getattr(self.metadata_service, "git_commit", None)
        self._config_hash = getattr(self.metadata_service, "config_hash", None)

        self.validation_service = (
            validation_service_factory(self)
            if validation_service_factory is not None
            else None
        )
        self.write_service = write_service_factory(self) if write_service_factory is not None else None

    # Lifecycle -----------------------------------------------------------
    def run(
        self,
        output_dir: Path,
        *,
        run_tag: str | None = None,
        mode: str | None = None,
        extended: bool = False,
        dry_run: bool | None = None,
        sample: int | None = None,
        limit: int | None = None,
        include_qc_metrics: bool = False,
        fail_on_schema_drift: bool = True,
    ) -> RunResult:
        if dry_run is not None:
            self.dry_run = dry_run

        logger = UnifiedLogger.get(self.__class__.__name__).bind(
            run_id=self.run_id, pipeline=self.pipeline_code
        )
        options = StageExecutionOptions(
            run_tag=run_tag,
            mode=mode,
            extended=extended,
            dry_run=self.dry_run,
            sample=sample,
            limit=limit,
            include_qc_metrics=include_qc_metrics,
            fail_on_schema_drift=fail_on_schema_drift,
        )

        run_state = RunState()

        logger.info("STAGE_RUN_START", stage="prepare_run")
        self.prepare_run(options)

        target_dir, artifacts = self.plan_run_artifacts(output_dir, run_tag, mode)
        run_state.artifacts = artifacts
        stage_context = StageContext(
            logger=logger,
            request_id=self.run_id,
            config=self.config,
            output_dir=target_dir,
            artifacts=artifacts,
            pipeline=self,
        )

        stage_descriptors = self.build_stage_plan(stage_context, options)
        stage_factory = self.create_stage_factory()
        stages = stage_factory.build(stage_descriptors, stage_context, options)
        self.stage_plan = stages
        durations, error = self.orchestration_service.execute(stages, stage_context, options)
        run_state.durations = durations
        run_state.error = error
        run_state.artifacts = stage_context.artifacts or run_state.artifacts

        qc_path: Path | None = None
        if run_state.error is None and self.qc_service is not None:
            try:
                qc_path = self.qc_service.execute(stage_context, options)
            except Exception as exc:  # pragma: no cover - surfaced via RunResult
                run_state.error = str(exc)
                if logger:
                    logger.error("QC_METRICS_ERROR", error=run_state.error)

        if options.extended and self.dry_run:
            metadata_writer = None
            if self.write_service is not None:
                metadata_writer = getattr(self.write_service, "write_metadata", None)
                if callable(metadata_writer):
                    metadata_writer(target_dir, artifacts, stage_context.current_df, dry_run=True)
            if metadata_writer is None:
                legacy_writer = getattr(self, "_write_metadata", None)
                if callable(legacy_writer):  # pragma: no cover - defensive
                    legacy_writer(target_dir, stage_context.current_df)

        result_frame = stage_context.current_df
        rows = 0 if not isinstance(result_frame, pd.DataFrame) else int(result_frame.shape[0])
        success = run_state.error is None
        metadata = self.build_run_metadata(stage_context, stages, run_state.durations, run_tag, mode)
        metadata["rows"] = rows
        if qc_path is not None:
            metadata["qc_metrics_path"] = str(qc_path)

        run_result = RunResult(
            success=success,
            rows=rows,
            artifacts=RunArtifacts(
                output_dir=target_dir,
                logs_directory=self.resolve_logs_directory(target_dir),
                write_artifacts=run_state.artifacts or WriteArtifacts(),
                qc_metrics_path=qc_path,
            ),
            duration_ms=run_state.durations,
            error=run_state.error,
            metadata=metadata,
        )
        self.finalize_run(run_result)
        logger.info("STAGE_RUN_END", stage="pipeline", success=success)
        return run_result

    # Hooks ---------------------------------------------------------------
    def prepare_run(self, options: StageExecutionOptions) -> None:  # pragma: no cover - optional hook
        """Вызывается перед началом extract."""

    def finalize_run(self, run_result: RunResult) -> None:  # pragma: no cover
        """Вызывается после завершения write."""

    # Planning ------------------------------------------------------------
    def create_stage_factory(self) -> StageFactory:
        return StageFactory(self)

    @abstractmethod
    def build_stage_plan(
        self, context: StageContext, options: StageExecutionOptions
    ) -> tuple[StageDescriptor, ...]:
        """Construct a deterministic stage descriptor plan for the pipeline."""

    def plan_run_artifacts(
        self, output_dir: Path, run_tag: str | None, mode: str | None
    ) -> tuple[Path, WriteArtifacts]:
        return self.orchestration_service.plan_run_artifacts(
            output_dir, self.pipeline_code, run_tag, mode
        )

    def build_run_stem(self, run_tag: str | None, mode: str | None) -> str:
        suffix = [self.pipeline_code]
        if mode:
            suffix.append(mode)
        if run_tag:
            suffix.append(run_tag)
        return self.deterministic_folder_prefix + "-".join(suffix)

    def build_run_metadata(
        self,
        context: StageContext,
        stage_plan: Iterable[StageCommand],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
    ) -> dict[str, Any]:
        if self.metadata_service is not None:
            return self.metadata_service.build(context, stage_plan, durations, run_tag, mode)
        if self.run_metadata_builder is not None:
            return self.run_metadata_builder.build(context, stage_plan, durations, run_tag, mode)
        return {}

    def resolve_logs_directory(self, output_dir: Path) -> Path:
        return output_dir / "logs"

    # Metadata ------------------------------------------------------------
    def _resolve_pipeline_code(
        self, config: Mapping[str, Any] | Any, definition: PipelineDefinition | None
    ) -> str:
        pipeline_name = None
        if definition is not None:
            pipeline_name = getattr(definition, "metadata", None)
            if pipeline_name and isinstance(pipeline_name, Mapping):
                pipeline_name = pipeline_name.get("name")
            elif hasattr(definition, "name"):
                pipeline_name = getattr(definition, "name")

        if pipeline_name:
            return str(pipeline_name)

        pipeline = getattr(config, "pipeline", None)
        if pipeline is not None and getattr(pipeline, "name", None):
            return str(pipeline.name)
        return self.__class__.__name__

    def _build_default_definition(self, config: Mapping[str, Any] | Any) -> PipelineDefinition:
        pipeline = getattr(config, "pipeline", None)
        name = getattr(pipeline, "name", None) if pipeline is not None else None
        resolved_name = str(name) if name else self.__class__.__name__
        return PipelineDefinition(name=resolved_name, runtime_factory=self.__class__)

    # Status --------------------------------------------------------------
    def stop(self) -> None:  # pragma: no cover - lifecycle hook
        """Gracefully stop pipeline execution if supported."""

    def status(self) -> Mapping[str, Any]:  # pragma: no cover - lifecycle hook
        """Return runtime status details."""
        return {}


__all__ = [
    "PipelineRuntimeBase",
    "StagePlanExecutor",
]
