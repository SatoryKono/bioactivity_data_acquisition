"""Shared runtime primitives for orchestrating ETL pipelines."""
from __future__ import annotations

# pylint: disable=undefined-variable

import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, cast

import pandas as pd

from bioetl.core.logging import UnifiedLogger

from bioetl.core.pipeline.definition import PipelineDefinition
from bioetl.core.pipeline import factories
from bioetl.core.pipeline.factory import StageFactory
from bioetl.core.pipeline.services import (
    ArtifactRuntimeService,
    ContextBuilder,
    MetadataRuntimeService,
    OrchestrationService,
    QCService,
    StagePlanExecutor,
    ValidationService,
    WriteService,
    default_context_builder_factory,
)
from bioetl.core.pipeline.types import (
    ArtifactStore,
    DataBucket,
    DefaultArtifactContext,
    DefaultDomainContext,
    DefaultExecutionContext,
    DefaultInfrastructureContext,
    PipelineBaseProtocol,
    RunResult,
    RunState,
    StageCommand,
    StageContext,
    StageDescriptor,
    StageExecutionOptions,
    StageProtocol,
    WriteArtifacts,
)
from bioetl.core.pipeline.artifact_runtime_builder import (
    ArtifactRuntimeBuilder,
    ArtifactRuntimeBuilderProtocol,
)
from bioetl.core.pipeline.metadata_runtime_builder import (
    MetadataRuntimeBuilder,
)
from bioetl.core.runtime import LifecycleCoordinator, OrchestrationCoordinator
from bioetl.core.pipeline.qc_runtime_builder import QCRuntimeBuilder


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
        stage_plan_executor: StagePlanExecutor | None = None,
        orchestration_service_factory: Callable[
            [OrchestrationCoordinator], OrchestrationService
        ]
        | None = None,
        artifact_runtime_service_factory: Callable[
            ["PipelineRuntimeBase"], ArtifactRuntimeService
        ]
        | None = None,
        artifact_runtime_service: ArtifactRuntimeService | None = None,
        artifact_runtime_builder: ArtifactRuntimeBuilderProtocol | None = None,
        qc_runtime_service_factory: Callable[
            ["PipelineRuntimeBase"], Any
        ]
        | None = None,
        qc_runtime_builder: QCRuntimeBuilder | None = None,
        metadata_runtime_service_factory: Callable[
            ["PipelineRuntimeBase"], MetadataRuntimeService
        ]
        | None = None,
        metadata_runtime_builder: MetadataRuntimeBuilder | None = None,
        context_builder: ContextBuilder | None = None,
        context_builder_factory: Callable[
            ["PipelineRuntimeBase"], ContextBuilder
        ]
        | None = None,
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

        artifact_runtime_factory = (
            artifact_runtime_service_factory
            or factories.default_artifact_runtime_service_factory(
                artifact_runtime_builder=artifact_runtime_builder,
                artifact_runtime_service=artifact_runtime_service,
            )
        )
        self.artifact_runtime_service = (
            artifact_runtime_service or artifact_runtime_factory(self)
        )
        self.artifact_planner = self.artifact_runtime_service.artifact_planner
        self.artifact_service = self.artifact_runtime_service.artifact_service

        qc_runtime_factory = (
            qc_runtime_service_factory
            or factories.default_qc_runtime_service_factory(
                stage_plan_executor=stage_plan_executor,
                qc_runtime_builder=qc_runtime_builder,
            )
        )
        self.qc_runtime_service, self.qc_coordinator = qc_runtime_factory(self)
        metadata_runtime_factory = (
            metadata_runtime_service_factory
            or factories.default_metadata_runtime_service_factory(
                config=config,
                pipeline_code=self.pipeline_code,
                logs_directory_resolver=self.resolve_logs_directory,
                metadata_runtime_builder=metadata_runtime_builder,
            )
        )
        (
            self.metadata_runtime_service,
            self.metadata_coordinator,
        ) = metadata_runtime_factory(self)
        self.metadata_service = self.metadata_coordinator.metadata_service
        self.run_metadata_builder = getattr(
            self.metadata_service,
            "builder",
            None,
        )
        self._git_commit = self.metadata_coordinator.git_commit
        self._config_hash = self.metadata_coordinator.config_hash

        self.stage_plan_executor = self.qc_coordinator.stage_plan_executor
        orchestration_factory = (
            orchestration_service_factory
            or factories.default_orchestration_service_factory(
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
        context_factory = (
            context_builder_factory or default_context_builder_factory()
        )
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
        return cast(
            RunResult,
            self.lifecycle.run(
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
            ),
        )

    def _prepare_stage_context(
        self,
        output_dir: Path,
        options: StageExecutionOptions,
        logger: UnifiedLogger,
    ) -> tuple[StageContext, RunState, Path]:
        """Собирает состояние запуска и StageContext перед
        исполнением плана.
        """

        run_state = RunState()

        any_logger = cast(Any, logger)
        any_logger.info("STAGE_RUN_START", stage="prepare_run")
        self.prepare_run(options)

        plan_run = self.artifact_runtime_service.plan_run_artifacts
        target_dir, artifacts = plan_run(
            output_dir,
            self.pipeline_code,
            options.run_tag,
            options.mode,
        )
        run_state.artifacts = artifacts

        data_bucket = DataBucket()
        artifact_store = ArtifactStore(artifacts)
        execution_context = DefaultExecutionContext(
            logger=logger,
            request_id=self.run_id,
            trace_id=self.run_id,
        )
        stage_context = self.context_builder.build(
            execution=execution_context,
            domain=DefaultDomainContext(pipeline=self),
            infrastructure=DefaultInfrastructureContext(
                output_dir=target_dir,
                metadata_service=self.metadata_coordinator.metadata_service,
                qc_orchestrator=self.qc_coordinator.qc_orchestrator,
            ),
            artifacts=DefaultArtifactContext(
                data_bucket=data_bucket,
                artifact_store=artifact_store,
            ),
        )

        return stage_context, run_state, target_dir

    def _execute_stage_plan(
        self,
        stage_context: StageContext,
        options: StageExecutionOptions,
    ) -> tuple[Iterable[StageCommand], dict[str, int], str | None]:
        """Формирует и исполняет план стадий,
        возвращая длительности и ошибку.
        """

        stage_descriptors = self.build_stage_plan(stage_context, options)
        stage_factory = self.create_stage_factory()
        stages = stage_factory.build(
            stage_descriptors,
            stage_context,
            options,
        )  # type: ignore[arg-type]
        self.stage_plan = stages
        durations, error = self.orchestration_service.execute(
            stages,
            stage_context,  # type: ignore[arg-type]
            options,
        )
        return stages, durations, error

    def _run_qc(
        self,
        stage_context: StageContext,
        options: StageExecutionOptions,
        run_state: RunState,
        logger: UnifiedLogger,
    ) -> Path | None:
        """Запускает QC-этап, обновляя состояние выполнения."""

        if run_state.error is not None:
            return None

        result = self.qc_coordinator.qc_runtime_service.run(
            stage_context,
            options,
        )
        qc_path, qc_error = cast(
            tuple[Path | None, str | None],
            result,
        )
        if qc_error is not None:
            run_state.error = qc_error
            any_logger = cast(Any, logger)
            any_logger.error(
                "QC_METRICS_ERROR",
                error=run_state.error,
            )
        return qc_path

    def _build_run_result(
        self,
        stage_context: StageContext,
        stages: Iterable[StageCommand],
        run_state: RunState,
        target_dir: Path,
        options: StageExecutionOptions,
        qc_path: Path | None,
    ) -> RunResult:
        """Собирает итоговый RunResult и инициирует финализацию."""

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
                        run_state.artifacts,
                        stage_context.data_bucket.get(),
                        dry_run=True,
                    )
            if metadata_writer is None:
                legacy_writer: Callable[..., Any] | None = getattr(
                    self,
                    "_write_metadata",
                    None,
                )
                if legacy_writer is not None:  # pragma: no cover - defensive
                    legacy_writer(
                        target_dir,
                        stage_context.data_bucket.get(),
                    )

        result_frame = stage_context.data_bucket.get()
        rows = 0
        if isinstance(result_frame, pd.DataFrame) and not self.dry_run:
            rows = int(result_frame.shape[0])
        success = run_state.error is None
        metadata_runtime_service = (
            self.metadata_coordinator.metadata_runtime_service
        )
        logs_directory_resolver = (
            self.metadata_coordinator.logs_directory_resolver
        )
        run_result = cast(
            RunResult,
            metadata_runtime_service.build_run_result(
                context=stage_context,
                stage_plan=stages,
                run_state=run_state,
                run_tag=options.run_tag,
                mode=options.mode,
                rows=rows,
                qc_metrics_path=qc_path,
                success=success,
                output_dir=target_dir,
                logs_directory=logs_directory_resolver(
                    target_dir
                ),
            ),
        )
        self.finalize_run(run_result)
        return run_result

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
        stage_plan: Iterable[StageProtocol],
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
                return cast(
                    "dict[str, Any]",
                    builder(
                        context,
                        stage_plan,
                        durations,
                        run_tag,
                        mode,
                        rows=rows,
                        qc_metrics_path=qc_metrics_path,
                    ),
                )
            return cast(
                "dict[str, Any]",
                self.metadata_service.build(
                    context,
                    stage_plan,
                    durations,
                    run_tag,
                    mode,
                ),
            )
        metadata_runtime_service = self.metadata_runtime_service
        return cast(
            "dict[str, Any]",
            metadata_runtime_service.build_run_metadata(
                context,
                stage_plan,
                durations,
                run_tag,
                mode,
                rows=rows,
                qc_metrics_path=qc_metrics_path,
            ),
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
    "ArtifactRuntimeBuilder",
    "QCRuntimeBuilder",
    "MetadataRuntimeBuilder",
    "StagePlanExecutor",
]
