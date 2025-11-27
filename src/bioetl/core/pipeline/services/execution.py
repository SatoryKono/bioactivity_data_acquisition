"""Execution services for pipeline stages."""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, cast

import pandas as pd

from bioetl.core.pipeline.services.artifacts import (
    ArtifactService,
    default_artifact_service_factory,
)
from bioetl.core.pipeline.types import (
    StageCommand,
    StageContextProtocol,
    StageExecutionOptions,
    StageRuntimeContext,
    WriteArtifacts,
)
from bioetl.core.runtime.lifecycle import OrchestrationCoordinatorProtocol
from bioetl.core.runtime.qc import QCOrchestratorProtocol


class StagePlanExecutor:
    """Responsible for executing the stage plan and tracking durations."""

    def __init__(
        self, qc_orchestrator: QCOrchestratorProtocol | None = None
    ) -> None:
        self.qc_orchestrator = qc_orchestrator

    def execute(
        self,
        stages: Iterable[StageCommand],
        context: StageContextProtocol,
        options: StageExecutionOptions,
        runtime_context: StageRuntimeContext | None = None,
    ) -> tuple[dict[str, int], str | None]:
        """
        Execute a sequence of stages.

        Args:
            stages: The list of stage commands to execute.
            context: The shared stage context.
            options: Execution options.
            runtime_context: Optional pre-configured runtime context.

        Returns:
            A tuple containing a dictionary of durations (stage_name -> ms)
            and an error message string if failure occurred (otherwise None).
        """
        logger = context.logger
        any_logger = cast(Any, logger)
        durations: dict[str, int] = {}
        error: str | None = None
        runtime_context = runtime_context or StageRuntimeContext(
            context=context,
            options=options,
        )
        runtime_context.context = context
        runtime_context.options = options

        for stage in stages:
            started = time.perf_counter()
            if logger:
                any_logger.info("STAGE_RUN_START", stage=stage.name)
            try:
                result = stage.execute(runtime_context)
                if isinstance(result.output, pd.DataFrame):
                    context.data_bucket.set(result.output)
                if (
                    stage.name == "extract"
                    and isinstance(result.output, pd.DataFrame)
                ):
                    context.metadata["extract_rows"] = int(
                        result.output.shape[0]
                    )
                if (
                    not options.dry_run
                    and options.sample is not None
                    and options.sample > 0
                    and isinstance(context.current_df, pd.DataFrame)
                    and stage.name in ("extract", "transform", "validate")
                ):
                    context.current_df = context.current_df.head(
                        options.sample
                    )
                if (
                    stage.name == "save_results"
                    and hasattr(result.output, "artifacts")
                ):
                    artifacts = (
                        result.output.artifacts  # type: ignore[attr-defined]
                    )
                    if isinstance(artifacts, WriteArtifacts):
                        context.artifact_store.set(artifacts)
            except Exception as exc:  # noqa: BLE001
                # pylint: disable=broad-except
                # pylint: disable=broad-exception-caught
                error = str(exc)
                if logger:
                    any_logger.error(
                        "STAGE_RUN_ERROR",
                        stage=stage.name,
                        error=error,
                    )
                break
            finally:
                duration_ms = int((time.perf_counter() - started) * 1000)
                durations[stage.name] = duration_ms
                if logger:
                    any_logger.info(
                        "STAGE_RUN_END",
                        stage=stage.name,
                        duration_ms=duration_ms,
                    )

        return durations, error


@dataclass(slots=True)
class OrchestrationService:
    """Orchestration of stages and artifact planning."""

    stage_plan_executor: StagePlanExecutor
    artifact_service: ArtifactService

    def execute(
        self,
        stages: Iterable[StageCommand],
        context: StageContextProtocol,
        options: StageExecutionOptions,
        runtime_context: StageRuntimeContext | None = None,
    ) -> tuple[dict[str, int], str | None]:
        """
        Execute the pipeline stages.

        Delegates to StagePlanExecutor.
        """
        return self.stage_plan_executor.execute(
            stages,
            context,
            options,
            runtime_context,
        )

    def plan_run_artifacts(
        self,
        output_dir: Path,
        pipeline_code: str,
        run_tag: str | None,
        mode: str | None,
    ) -> tuple[Path, WriteArtifacts]:
        """
        Plan artifacts for the run.

        Delegates to ArtifactService.
        """
        return self.artifact_service.plan_run_artifacts(
            output_dir,
            pipeline_code,
            run_tag,
            mode,
        )


def default_orchestration_service_factory(
    stage_plan_executor: StagePlanExecutor | None = None,
    artifact_service: ArtifactService | None = None,
) -> Callable[[OrchestrationCoordinatorProtocol], OrchestrationService]:
    """Create a factory for the default orchestration service."""
    def _factory(
        coordinator: OrchestrationCoordinatorProtocol,
    ) -> OrchestrationService:
        executor = stage_plan_executor or getattr(
            coordinator, "stage_plan_executor", None
        )
        artifacts = artifact_service or getattr(
            coordinator, "artifact_service", None
        )
        return OrchestrationService(
            stage_plan_executor=executor or StagePlanExecutor(),
            artifact_service=artifacts or default_artifact_service_factory(),
        )

    return _factory
