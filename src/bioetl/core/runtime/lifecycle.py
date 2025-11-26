"""Координация жизненного цикла исполнения пайплайна."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.types import (
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
        self, output_dir: Path, pipeline_code: str, run_tag: str | None, mode: str | None
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

    pipeline: object
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

        stage_context, run_state, target_dir = self.pipeline._prepare_stage_context(
            output_dir=output_dir,
            options=options,
            logger=logger,
        )

        stages, durations, error = self.pipeline._execute_stage_plan(
            stage_context, options
        )
        run_state.durations = durations
        run_state.error = error
        run_state.artifacts = stage_context.artifact_store.get() or run_state.artifacts

        qc_path = self.pipeline._run_qc(stage_context, options, run_state, logger)

        run_result = self.pipeline._build_run_result(
            stage_context=stage_context,
            stages=stages,
            run_state=run_state,
            target_dir=target_dir,
            options=options,
            qc_path=qc_path,
        )
        return run_result


__all__ = [
    "LifecycleCoordinator",
    "OrchestrationCoordinator",
    "OrchestrationCoordinatorProtocol",
]
