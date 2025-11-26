"""Координация жизненного цикла исполнения пайплайна."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import pandas as pd

from bioetl.core.logging import UnifiedLogger
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
    StageExecutionOptions,
)
from bioetl.core.runtime.metadata import MetadataCoordinator
from bioetl.core.runtime.qc import QCCoordinator


class OrchestrationCoordinatorProtocol(Protocol):
    """Минимальный интерфейс оркестрации стадий."""

    def execute(
        self,
        stages,
        context,
        options,
    ) -> tuple[dict[str, int], str | None]:
        ...

    def plan_run_artifacts(
        self,
        output_dir: Path,
        pipeline_code: str,
        run_tag: str | None,
        mode: str | None,
    ) -> tuple[Path, object]:
        ...


@dataclass(slots=True)
class OrchestrationCoordinator:
    """Небольшая обертка над сервисами оркестрации."""

    stage_plan_executor: object
    artifact_service: object


@dataclass(slots=True)
class LifecycleCoordinator:
    """Делегат для запуска пайплайна."""

    pipeline: PipelineBaseProtocol
    orchestration_service: OrchestrationCoordinatorProtocol
    metadata_coordinator: MetadataCoordinator
    qc_coordinator: QCCoordinator
    context_builder: object
    artifact_runtime_service: object

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
            self.pipeline.dry_run = dry_run

        logger = UnifiedLogger.get(self.pipeline.__class__.__name__).bind(
            run_id=self.pipeline.run_id,
            pipeline=self.pipeline.pipeline_code,
        )
        options = StageExecutionOptions(
            run_tag=run_tag,
            mode=mode,
            extended=extended,
            dry_run=self.pipeline.dry_run,
            sample=sample,
            limit=limit,
            include_qc_metrics=include_qc_metrics,
            fail_on_schema_drift=fail_on_schema_drift,
            enable_validation=enable_validation,
        )

        run_state = RunState()

        logger.info("STAGE_RUN_START", stage="prepare_run")
        self.pipeline.prepare_run(options)

        target_dir, artifacts = (
            self.artifact_runtime_service.plan_run_artifacts(
                output_dir,
                self.pipeline.pipeline_code,
                run_tag,
                mode,
            )
        )
        run_state.artifacts = artifacts
        data_bucket = DataBucket()
        artifact_store = ArtifactStore(artifacts)

        execution = DefaultExecutionContext(
            logger=logger,
            request_id=self.pipeline.run_id,
        )
        domain = DefaultDomainContext(
            pipeline=self.pipeline,
        )
        infrastructure = DefaultInfrastructureContext(
            output_dir=target_dir,
            metadata_service=self.metadata_coordinator.metadata_service,
            qc_orchestrator=self.qc_coordinator.qc_orchestrator,
        )
        artifacts_context = DefaultArtifactContext(
            data_bucket=data_bucket,
            artifact_store=artifact_store,
        )

        stage_context = self.context_builder.build(
            execution=execution,
            domain=domain,
            infrastructure=infrastructure,
            artifacts=artifacts_context,
        )

        stage_descriptors = self.pipeline.build_stage_plan(stage_context, options)
        stage_factory = self.pipeline.create_stage_factory()
        stages = stage_factory.build(stage_descriptors, stage_context, options)
        self.pipeline.stage_plan = stages
        durations, error = (
            self.orchestration_service.execute(
                stages,
                stage_context,
                options,
            )
        )
        run_state.durations = durations
        run_state.error = error
        run_state.artifacts = (
            stage_context.artifact_store.get() or run_state.artifacts
        )

        qc_path: Path | None = None
        if run_state.error is None:
            qc_path, qc_error = (
                self.qc_coordinator.qc_runtime_service.run(
                    stage_context, options
                )
            )
            if qc_error is not None:
                run_state.error = qc_error
                if logger:
                    logger.error("QC_METRICS_ERROR", error=run_state.error)

        if options.extended and self.pipeline.dry_run:
            metadata_writer = None
            if self.pipeline.write_service is not None:
                metadata_writer = getattr(
                    self.pipeline.write_service,
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
                    self.pipeline,
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
        is_dataframe = isinstance(result_frame, pd.DataFrame)
        if is_dataframe and not self.pipeline.dry_run:
            rows = int(result_frame.shape[0])
        success = run_state.error is None
        metadata_service = self.metadata_coordinator.metadata_runtime_service
        logs_resolver = self.metadata_coordinator.logs_directory_resolver
        run_result = (
            metadata_service.build_run_result(
                context=stage_context,
                stage_plan=stages,
                run_state=run_state,
                run_tag=run_tag,
                mode=mode,
                rows=rows,
                qc_metrics_path=qc_path,
                success=success,
                output_dir=target_dir,
                logs_directory=logs_resolver(target_dir),
            )
        )
        self.pipeline.finalize_run(run_result)
        return run_result


__all__ = [
    "LifecycleCoordinator",
    "OrchestrationCoordinator",
    "OrchestrationCoordinatorProtocol",
]
