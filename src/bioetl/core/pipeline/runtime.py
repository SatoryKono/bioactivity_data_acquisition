"""Shared runtime primitives for orchestrating ETL pipelines."""
from __future__ import annotations

# pylint: disable=undefined-variable

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
    ArtifactRuntimeService,
    ContextBuilder,
    MetadataRuntimeService,
    MetadataService,
    OrchestrationService,
    QCOrchestrator,
    QCService,
    RunMetadataBuilder,
    StagePlanExecutor,
    ValidationService,
    WriteService,
    default_artifact_runtime_service_factory,
    default_context_builder_factory,
    default_metadata_runtime_service_factory,
    default_orchestration_service_factory,
    default_qc_runtime_service_factory,
)
from bioetl.core.pipeline.types import (
    ArtifactStore,
    DataBucket,
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
        validation_service_factory: Callable[
            ["PipelineRuntimeBase"], ValidationService
        ]
        | None = None,
        write_service_factory: Callable[
            ["PipelineRuntimeBase"], WriteService
        ]
        | None = None,
        qc_executor_factory: Callable[[], QCMetricsExecutor] | None = None,
        qc_plan: QCPlan | None = None,
        qc_thresholds: Mapping[str, float] | None = None,
        qc_dry_run: bool | None = None,
        qc_enabled: bool | None = None,
        stage_plan_executor: StagePlanExecutor | None = None,
        artifact_planner: ArtifactPlanner | None = None,
        qc_service: QCService | None = None,
        metadata_service: MetadataService | None = None,
        metadata_runtime_service: MetadataRuntimeService | None = None,
        metadata_service_factory: Callable[["PipelineRuntimeBase"], MetadataService]
        | None = None,
        orchestration_service_factory: Callable[
            ["PipelineRuntimeBase"], OrchestrationService
        ]
        | None = None,
        qc_service_factory: Callable[
            ["PipelineRuntimeBase"], QCService
        ]
        | None = None,
        metadata_runtime_service_factory: Callable[
            ["PipelineRuntimeBase"], MetadataRuntimeService
        ]
        | None = None,
        qc_runtime_service: Any | None = None,
        qc_runtime_service_factory: Callable[["PipelineRuntimeBase"], Any] | None = None,
        artifact_runtime_service: ArtifactRuntimeService | None = None,
        artifact_runtime_service_factory: Callable[
            ["PipelineRuntimeBase"], ArtifactRuntimeService
        ]
        | None = None,
        context_builder: ContextBuilder | None = None,
        context_builder_factory: Callable[["PipelineRuntimeBase"], ContextBuilder]
        | None = None,
        run_metadata_builder: RunMetadataBuilder | None = None,
    ) -> None:
        self.config = config
        self.pipeline_definition = (
            pipeline_definition or self._build_default_definition(config)
        )
        self.run_id = run_id or uuid.uuid4().hex
        self.validator = validator
        self.pipeline_code = self._resolve_pipeline_code(
            config,
            self.pipeline_definition,
        )
        materialization = getattr(config, "materialization", None)
        root = getattr(materialization, "root", None)
        self.output_root = Path(root) if root else Path.cwd()
        self.logs_directory = (
            self.output_root.parent / "logs" / self.pipeline_code
        )
        self.dry_run = False

        artifact_runtime_factory = artifact_runtime_service_factory or default_artifact_runtime_service_factory(
            artifact_planner=artifact_planner
        )
        self.artifact_runtime_service = artifact_runtime_service or artifact_runtime_factory(self)
        self.artifact_planner = self.artifact_runtime_service.artifact_planner
        self.artifact_service = self.artifact_runtime_service.artifact_service

        qc_runtime_factory = qc_runtime_service_factory or default_qc_runtime_service_factory(
            qc_service=qc_service,
            qc_service_factory=qc_service_factory,
            qc_executor_factory=qc_executor_factory,
            qc_plan=qc_plan,
            qc_thresholds=qc_thresholds,
            qc_dry_run=qc_dry_run,
            qc_enabled=qc_enabled,
        )
        self.qc_runtime_service = qc_runtime_service or qc_runtime_factory(self)

        metadata_runtime_factory = metadata_runtime_service_factory or default_metadata_runtime_service_factory(
            config=config,
            pipeline_code=self.pipeline_code,
            metadata_service=metadata_service,
            metadata_service_factory=metadata_service_factory,
            run_metadata_builder=run_metadata_builder,
            logs_directory_resolver=self.resolve_logs_directory,
        )
        self.metadata_runtime_service = (
            metadata_runtime_service or metadata_runtime_factory(self)
        )
        self.metadata_service = self.metadata_runtime_service.metadata_service
        self.run_metadata_builder = getattr(self.metadata_service, "builder", None)
        self._git_commit = self.metadata_runtime_service.git_commit
        self._config_hash = self.metadata_runtime_service.config_hash

        self.stage_plan_executor = self._create_stage_executor(
            stage_plan_executor,
            self.qc_orchestrator,
        )
        orchestration_factory = (
            orchestration_service_factory
            or default_orchestration_service_factory(
                stage_plan_executor=self.stage_plan_executor,
                artifact_service=self.artifact_service,
            )
        )
        self.orchestration_service = orchestration_factory(self)

        self.validation_service = None
        if validation_service_factory is not None:
            self.validation_service = validation_service_factory(self)

        self.write_service = (
            write_service_factory(self)
            if write_service_factory is not None
            else None
        )
        self.stage_plan: Iterable[Any] | None = None
        context_factory = context_builder_factory or default_context_builder_factory()
        self.context_builder = context_builder or context_factory(self)

    @property
    def qc_service(self) -> QCService | None:
        """Access the underlying QC service."""
        return getattr(self.qc_runtime_service, "qc_service", None)

    @qc_service.setter
    def qc_service(self, value: QCService | None) -> None:
        """Update the underlying QC service and propagate to orchestrator."""
        if self.qc_runtime_service:
            self.qc_runtime_service.qc_service = value
            if value is not None:
                if self.qc_runtime_service.qc_orchestrator:
                    self.qc_runtime_service.qc_orchestrator.qc_service = value
                else:
                    self.qc_runtime_service.qc_orchestrator = QCOrchestrator(value)
            else:
                self.qc_runtime_service.qc_orchestrator = None

    @property
    def qc_orchestrator(self) -> QCOrchestrator | None:
        """Access the underlying QC orchestrator."""
        return getattr(self.qc_runtime_service, "qc_orchestrator", None)

    def _create_stage_executor(
        self,
        stage_plan_executor: StagePlanExecutor | None,
        qc_orchestrator: Any,
    ) -> StagePlanExecutor:
        if stage_plan_executor is not None:
            stage_plan_executor.qc_orchestrator = qc_orchestrator
            return stage_plan_executor
        return StagePlanExecutor(qc_orchestrator)

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
        enable_validation: bool = True,
    ) -> RunResult:
        if dry_run is not None:
            self.dry_run = dry_run

        logger = UnifiedLogger.get(self.__class__.__name__).bind(
            run_id=self.run_id,
            pipeline=self.pipeline_code,
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
            enable_validation=enable_validation,
        )

        run_state = RunState()

        logger.info("STAGE_RUN_START", stage="prepare_run")
        self.prepare_run(options)

        target_dir, artifacts = self.artifact_runtime_service.plan_run_artifacts(
            output_dir,
            self.pipeline_code,
            run_tag,
            mode,
        )
        run_state.artifacts = artifacts
        data_bucket = DataBucket()
        artifact_store = ArtifactStore(artifacts)
        stage_context = self.context_builder.build(
            logger=logger,
            output_dir=target_dir,
            data_bucket=data_bucket,
            artifact_store=artifact_store,
            metadata_service=self.metadata_service,
            qc_orchestrator=self.qc_orchestrator,
        )

        stage_descriptors = self.build_stage_plan(stage_context, options)
        stage_factory = self.create_stage_factory()
        stages = stage_factory.build(stage_descriptors, stage_context, options)
        self.stage_plan = stages
        durations, error = self.orchestration_service.execute(
            stages,
            stage_context,
            options,
        )
        run_state.durations = durations
        run_state.error = error
        run_state.artifacts = (
            stage_context.artifact_store.get() or run_state.artifacts
        )

        qc_path: Path | None = None
        if run_state.error is None:
            qc_path, qc_error = self.qc_runtime_service.run(stage_context, options)
            if qc_error is not None:
                run_state.error = qc_error
                if logger:
                    logger.error("QC_METRICS_ERROR", error=run_state.error)

        if options.extended and self.dry_run:
            metadata_writer = None
            if self.write_service is not None:
                metadata_writer = getattr(
                    self.write_service,
                    "write_metadata",
                    None,
                )
                if callable(metadata_writer):
                    metadata_writer(
                        target_dir,
                        artifacts,
                        stage_context.data_bucket.get(),
                        dry_run=True,
                    )
            if metadata_writer is None:
                legacy_writer = getattr(
                    self,
                    "_write_metadata",
                    None,
                )
                if callable(legacy_writer):  # pragma: no cover - defensive
                    legacy_writer(
                        target_dir,
                        stage_context.data_bucket.get(),
                    )

        result_frame = stage_context.data_bucket.get()
        rows = 0
        if isinstance(result_frame, pd.DataFrame) and not self.dry_run:
            rows = int(result_frame.shape[0])
        success = run_state.error is None
        run_result = self.metadata_runtime_service.build_run_result(
            context=stage_context,
            stage_plan=stages,
            run_state=run_state,
            run_tag=run_tag,
            mode=mode,
            rows=rows,
            qc_metrics_path=qc_path,
            success=success,
            output_dir=target_dir,
            logs_directory=self.resolve_logs_directory(target_dir),
        )
        self.finalize_run(run_result)
        logger.info("STAGE_RUN_END", stage="pipeline", success=success)
        return run_result

    def _build_config_provider(self) -> Callable[[str], Any]:
        def _resolver(key: str) -> Any:
            if hasattr(self.config, key):
                return getattr(self.config, key)
            if isinstance(self.config, Mapping) and key in self.config:
                return self.config[key]
            msg = f"Config key '{key}' not found"
            raise KeyError(msg)

        return _resolver

    def prepare_run(
        self,
        options: StageExecutionOptions,
    ) -> None:  # pragma: no cover - optional hook
        """Вызывается перед началом extract."""

    def finalize_run(  # pragma: no cover
        self,
        run_result: RunResult,
    ) -> None:
        """Вызывается после завершения write."""

    def create_stage_factory(self) -> StageFactory:
        """Create the default stage factory for this runtime."""
        return StageFactory(self)

    @abstractmethod
    def build_stage_plan(
        self, context: StageContext, options: StageExecutionOptions
    ) -> tuple[StageDescriptor, ...]:
        """Construct a deterministic stage descriptor plan for the pipeline."""

    def plan_run_artifacts(
        self, output_dir: Path, run_tag: str | None, mode: str | None
    ) -> tuple[Path, WriteArtifacts]:
        return self.artifact_runtime_service.plan_run_artifacts(
            output_dir, self.pipeline_code, run_tag, mode
        )

    def build_run_stem(self, run_tag: str | None, mode: str | None) -> str:
        """Construct the folder name stem for the run."""
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
        *,
        rows: int = 0,
        qc_metrics_path: Path | None = None,
    ) -> dict[str, Any]:
        if self.metadata_service is not None:
            builder = getattr(self.metadata_service, "build_for_run", None)
            if callable(builder):
                return builder(
                    context,
                    stage_plan,
                    durations,
                    run_tag,
                    mode,
                    rows=rows,
                    qc_metrics_path=qc_metrics_path,
                )
            return self.metadata_service.build(
                context,
                stage_plan,
                durations,
                run_tag,
                mode,
            )
        return self.metadata_runtime_service.build_run_metadata(
            context,
            stage_plan,
            durations,
            run_tag,
            mode,
            rows=rows,
            qc_metrics_path=qc_metrics_path,
        )

    def resolve_logs_directory(self, output_dir: Path) -> Path:
        return output_dir / "logs"

    def _resolve_pipeline_code(
        self,
        config: Mapping[str, Any] | Any,
        definition: PipelineDefinition | None,
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

    def _build_default_definition(
        self,
        config: Mapping[str, Any] | Any,
    ) -> PipelineDefinition:
        pipeline = getattr(config, "pipeline", None)
        name = None
        if pipeline is not None:
            name = getattr(pipeline, "name", None)
        resolved_name = str(name) if name else self.__class__.__name__
        return PipelineDefinition(
            name=resolved_name,
            runtime_factory=self.__class__,
        )

    def stop(self) -> None:  # pragma: no cover - lifecycle hook
        """Gracefully stop pipeline execution if supported."""

    def status(
        self,
    ) -> Mapping[str, Any]:  # pragma: no cover - lifecycle hook
        """Return runtime status details."""
        return {}


__all__ = [
    "PipelineRuntimeBase",
    "StagePlanExecutor",
]
