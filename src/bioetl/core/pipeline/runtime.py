"""Shared runtime primitives for orchestrating ETL pipelines."""
from __future__ import annotations

# pylint: disable=undefined-variable

import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from bioetl.core.pipeline.definition import PipelineDefinition
from bioetl.core.pipeline.factory import StageFactory
from bioetl.core.pipeline.services import (
    ArtifactPlanner,
    ArtifactRuntimeService,
    ContextBuilder,
    MetadataRuntimeService,
    MetadataService,
    OrchestrationService,
    QCService,
    RunMetadataBuilder,
    StagePlanExecutor,
    ValidationService,
    WriteService,
    default_artifact_runtime_service_factory,
    default_context_builder_factory,
    default_metadata_runtime_service_factory,
    default_orchestration_service_factory,
)
from bioetl.core.pipeline.types import (
    PipelineBaseProtocol,
    RunArtifacts,
    RunResult,
    StageCommand,
    StageContext,
    StageDescriptor,
    StageExecutionOptions,
    WriteArtifacts,
)
from bioetl.core.runtime import (
    LifecycleCoordinator,
    MetadataCoordinator,
    OrchestrationCoordinator,
    QCCoordinator,
)
from bioetl.core.runtime.qc import default_qc_runtime_service_factory
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
        metadata_service_factory: Callable[[MetadataCoordinator], MetadataService]
        | None = None,
        orchestration_service_factory: Callable[
            [OrchestrationCoordinator], OrchestrationService
        ]
        | None = None,
        qc_service_factory: Callable[[QCCoordinator], QCService]
        | None = None,
        metadata_runtime_service_factory: Callable[
            [MetadataCoordinator], MetadataRuntimeService
        ]
        | None = None,
        qc_runtime_service: Any | None = None,
        qc_runtime_service_factory: Callable[[QCCoordinator], Any] | None = None,
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
            qc_service_factory=qc_service_factory,
            qc_service=qc_service,
            qc_executor_factory=qc_executor_factory,
            qc_plan=qc_plan,
            qc_thresholds=qc_thresholds,
            qc_dry_run=qc_dry_run,
            qc_enabled=qc_enabled,
        )
        if qc_runtime_service is not None:
            self.qc_coordinator = QCCoordinator(
                qc_runtime_service=qc_runtime_service,
                stage_plan_executor=stage_plan_executor,
            )
        else:
            self.qc_coordinator = QCCoordinator.from_factory(
                qc_runtime_service_factory=qc_runtime_factory,
                stage_plan_executor=stage_plan_executor,
            )
        self.qc_runtime_service = self.qc_coordinator.qc_runtime_service

        metadata_runtime_factory = metadata_runtime_service_factory or default_metadata_runtime_service_factory(
            config=config,
            pipeline_code=self.pipeline_code,
            metadata_service=metadata_service,
            metadata_service_factory=metadata_service_factory,
            run_metadata_builder=run_metadata_builder,
            logs_directory_resolver=self.resolve_logs_directory,
        )
        if metadata_runtime_service is not None:
            self.metadata_coordinator = MetadataCoordinator(
                metadata_runtime_service=metadata_runtime_service,
                logs_directory_resolver=self.resolve_logs_directory,
            )
        else:
            self.metadata_coordinator = MetadataCoordinator.from_factory(
                metadata_runtime_service_factory=metadata_runtime_factory,
                logs_directory_resolver=self.resolve_logs_directory,
            )
        self.metadata_runtime_service = self.metadata_coordinator.metadata_runtime_service
        self.metadata_service = self.metadata_coordinator.metadata_service
        self.run_metadata_builder = getattr(self.metadata_service, "builder", None)
        self._git_commit = self.metadata_coordinator.git_commit
        self._config_hash = self.metadata_coordinator.config_hash

        self.stage_plan_executor = self.qc_coordinator.stage_plan_executor
        orchestration_factory = (
            orchestration_service_factory
            or default_orchestration_service_factory(
                stage_plan_executor=self.stage_plan_executor,
                artifact_service=self.artifact_service,
            )
        )
        orchestration_coordinator = OrchestrationCoordinator(
            stage_plan_executor=self.stage_plan_executor,
            artifact_service=self.artifact_service,
        )
        self.orchestration_service = orchestration_factory(orchestration_coordinator)

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

        self.lifecycle = LifecycleCoordinator(
            pipeline=self,
            orchestration_service=self.orchestration_service,
            metadata_coordinator=self.metadata_coordinator,
            qc_coordinator=self.qc_coordinator,
            context_builder=self.context_builder,
            artifact_runtime_service=self.artifact_runtime_service,
        )

    @property
    def qc_service(self) -> QCService | None:
        """Access the underlying QC service."""
        return getattr(self.qc_coordinator, "qc_service", None)

    @qc_service.setter
    def qc_service(self, value: QCService | None) -> None:
        """Update the underlying QC service and propagate to orchestrator."""
        if hasattr(self, "qc_coordinator"):
            self.qc_coordinator.qc_service = value

    @property
    def qc_orchestrator(self) -> Any:
        """Access the underlying QC orchestrator."""
        return getattr(self.qc_coordinator, "qc_orchestrator", None)

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
        return self.lifecycle.run(
            output_dir,
            run_tag=run_tag,
            mode=mode,
            extended=extended,
            dry_run=dry_run,
            sample=sample,
            limit=limit,
            include_qc_metrics=include_qc_metrics,
            fail_on_schema_drift=fail_on_schema_drift,
            enable_validation=enable_validation,
        )

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

    def prepare_run(
        self,
        options: StageExecutionOptions,
    ) -> None:  # pragma: no cover - optional hook
        """Hook executed before the pipeline stages start."""

    def _build_config_provider(self) -> Callable[[str], Any]:
        """Return a simple config accessor for :class:`StageContext`."""

        def _provider(key: str) -> Any:
            if isinstance(self.config, Mapping):
                return self.config.get(key)
            return getattr(self.config, key)

        return _provider

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
