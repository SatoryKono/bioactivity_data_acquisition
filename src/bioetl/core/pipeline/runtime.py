"""Shared runtime primitives for orchestrating ETL pipelines."""
from __future__ import annotations

import time
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
    QCService,
    RunMetadataBuilder,
    ValidationService,
    WriteService,
    default_artifact_planner_factory,
    default_metadata_service_factory,
    default_qc_service_factory,
)
from bioetl.core.pipeline.types import (
    PipelineBaseProtocol,
    RunArtifacts,
    RunResult,
    StageCommand,
    StageContext,
    StageDescriptor,
    StageExecutionOptions,
    StageRuntimeContext,
    WriteArtifacts,
)
from bioetl.core.pipeline.services import (
    ArtifactPlanner,
    DefaultArtifactPlanner,
    ValidationService,
    WriteService,
)
from bioetl.qc.executor import QCMetricsExecutor
from bioetl.qc.plan import QCPlan


class StagePlanExecutor:
    """Ответственный за исполнение плана стадий и подсчет длительностей."""

    def __init__(self, qc_service: QCService) -> None:
        self.qc_service = qc_service

    def execute(
        self,
        stages: Iterable[StageCommand],
        context: StageContext,
        options: StageExecutionOptions,
        *,
        include_qc_metrics: bool,
    ) -> tuple[dict[str, int], str | None, Path | None]:
        logger = context.logger
        durations: dict[str, int] = {}
        error: str | None = None
        artifacts = context.artifacts or WriteArtifacts()
        if not context.artifacts:
            context.artifacts = artifacts
        qc_metrics_path: Path | None = None

        runtime_context = StageRuntimeContext(context=context, options=options)

        for stage in stages:
            started = time.perf_counter()
            if logger:
                logger.info("STAGE_RUN_START", stage=stage.name)
            try:
                result = stage.execute(runtime_context)
                if stage.name == "extract" and isinstance(result.output, pd.DataFrame):
                    context.metadata["extract_rows"] = int(result.output.shape[0])
                if stage.name == "save_results" and hasattr(result.output, "artifacts"):
                    artifacts = result.output.artifacts  # type: ignore[attr-defined]
                    if isinstance(artifacts, WriteArtifacts):
                        context.artifacts = artifacts
            except Exception as exc:  # pragma: no cover - surfaced via RunResult
                error = str(exc)
                if logger:
                    logger.error("STAGE_RUN_ERROR", stage=stage.name, error=error)
                break
            finally:
                duration_ms = int((time.perf_counter() - started) * 1000)
                durations[stage.name] = duration_ms
                if logger:
                    logger.info("STAGE_RUN_END", stage=stage.name, duration_ms=duration_ms)

        if error is None and include_qc_metrics:
            try:
                qc_metrics_path = self.qc_service.execute(context, options)
            except Exception as exc:  # pragma: no cover - surfaced via RunResult
                error = str(exc)
                if logger:
                    logger.error("QC_METRICS_ERROR", error=error)

        return durations, error, qc_metrics_path


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
        stage_plan_executor: StagePlanExecutor | None = None,
        artifact_planner: ArtifactPlanner | None = None,
        qc_service: QCService | None = None,
        metadata_service: MetadataService | None = None,
        run_metadata_builder: RunMetadataBuilder | None = None,
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
        self.qc_plan = qc_plan
        self.dry_run = False
        self.artifact_planner = artifact_planner or default_artifact_planner_factory()
        self.qc_service = qc_service or default_qc_service_factory(
            qc_plan=qc_plan, executor_factory=qc_executor_factory
        )
        self.stage_plan_executor = stage_plan_executor or StagePlanExecutor(self.qc_service)
        self.metadata_service = metadata_service or default_metadata_service_factory(
            config, self.pipeline_code
        )
        self.run_metadata_builder = run_metadata_builder or getattr(
            self.metadata_service, "builder", None
        )
        self._git_commit = getattr(self.metadata_service, "git_commit", None)
        self._config_hash = getattr(self.metadata_service, "config_hash", None)
        self.validation_service = (
            validation_service_factory(self)
            if validation_service_factory is not None
            else None
        )
        self.write_service = (
            write_service_factory(self) if write_service_factory is not None else None
        )
        self.artifact_planner: ArtifactPlanner = artifact_planner or DefaultArtifactPlanner()

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

        logger.info("STAGE_RUN_START", stage="prepare_run")
        self.prepare_run(options)

        target_dir, artifacts = self.plan_run_artifacts(output_dir, run_tag, mode)
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
        durations: dict[str, int] = {}
        error: str | None = None
        qc_path: Path | None = None
        if not options.dry_run:
            durations, error, qc_path = self.stage_plan_executor.execute(
                stages,
                stage_context,
                options,
                include_qc_metrics=include_qc_metrics,
            )

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
        success = error is None
        metadata = self.build_run_metadata(stage_context, stages, durations, run_tag, mode)
        metadata["rows"] = rows
        if qc_path is not None:
            metadata["qc_metrics_path"] = str(qc_path)

        run_result = RunResult(
            success=success,
            rows=rows,
            artifacts=RunArtifacts(
                output_dir=target_dir,
                logs_directory=self.resolve_logs_directory(target_dir),
                write_artifacts=stage_context.artifacts or WriteArtifacts(),
                qc_metrics_path=qc_path,
            ),
            duration_ms=durations,
            error=error,
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

        return self.stage_factory.build(context, options)

    def plan_run_artifacts(
        self, output_dir: Path, run_tag: str | None, mode: str | None
    ) -> tuple[Path, WriteArtifacts]:
        return self.artifact_planner.plan(output_dir, self.pipeline_code, run_tag, mode)

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
            return self.metadata_service.build(
                context, stage_plan, durations, run_tag, mode
            )
        if self.run_metadata_builder is not None:
            return self.run_metadata_builder.build(
                context, stage_plan, durations, run_tag, mode
            )
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
