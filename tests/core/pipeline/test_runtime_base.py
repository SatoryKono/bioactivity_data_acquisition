from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from bioetl.core.pipeline.runtime import PipelineRuntimeBase, StagePlanExecutor
from bioetl.core.pipeline.types import PipelineStageCommand, StageContext, StageExecutionOptions, WriteArtifacts


class RetryableError(Exception):
    pass


class RecordingExecutor(StagePlanExecutor):
    def __init__(self, *, max_retries: int = 0) -> None:
        super().__init__()
        self.max_retries = max_retries
        self.calls: list[tuple[str, int]] = []

    def execute(
        self,
        stage_plan: tuple[PipelineStageCommand, ...],
        context: StageContext,
        options: StageExecutionOptions,
        *,
        include_qc_metrics: bool,
    ) -> tuple[dict[str, int], str | None, Path | None]:
        durations: dict[str, int] = {}
        error: str | None = None
        for command in stage_plan:
            attempts = 0
            while True:
                attempts += 1
                self.calls.append((command.name, attempts))
                try:
                    result = command.handler(context, options)
                    if isinstance(result, pd.DataFrame):
                        context.current_df = result
                    break
                except RetryableError as exc:
                    if attempts > self.max_retries:
                        error = str(exc)
                        context.current_df = None
                        break
                    continue
                except Exception as exc:  # pragma: no cover - defensive
                    error = str(exc)
                    context.current_df = None
                    break
            durations[command.name] = durations.get(command.name, 0) + 5
            if error:
                break
        return durations, error, None


@dataclass
class _ContextPayload:
    metadata: dict[str, Any]
    artifacts: WriteArtifacts


class DummyRuntime(PipelineRuntimeBase):
    def __init__(self, *, executor: RecordingExecutor) -> None:
        super().__init__({}, stage_plan_executor=executor)
        self.executor = executor
        self.calls: list[str] = []
        self.transform_attempts = 0

    def build_stage_plan(
        self, context: StageContext, options: StageExecutionOptions
    ) -> tuple[PipelineStageCommand, ...]:
        self.calls.append("build_stage_plan")

        def _extract(ctx: StageContext, _options: StageExecutionOptions) -> pd.DataFrame:
            self.calls.append("extract")
            ctx.metadata["extract"] = True
            return pd.DataFrame({"value": [1, 2]})

        def _transform(ctx: StageContext, _options: StageExecutionOptions) -> pd.DataFrame:
            self.calls.append("transform")
            # fail first attempt and succeed on retry
            self.transform_attempts += 1
            if self.transform_attempts == 1:
                raise RetryableError("transient")
            return ctx.current_df.assign(value=lambda s: s["value"] * 2)

        def _save(ctx: StageContext, _options: StageExecutionOptions) -> WriteArtifacts:
            self.calls.append("save_results")
            ctx.artifacts = ctx.artifacts or WriteArtifacts()
            ctx.artifacts.data_path = ctx.output_dir / "dataset.csv"
            return ctx.artifacts

        return (
            PipelineStageCommand("extract", _extract),
            PipelineStageCommand("transform", _transform),
            PipelineStageCommand("save_results", _save),
        )


def test_run_handles_retries_and_metadata(tmp_path: Path) -> None:
    executor = RecordingExecutor(max_retries=1)
    runtime = DummyRuntime(executor=executor)

    result = runtime.run(tmp_path)

    assert result.success is True
    assert result.rows == 2
    assert executor.calls == [
        ("extract", 1),
        ("transform", 1),
        ("transform", 2),
        ("save_results", 1),
    ]
    assert result.duration_ms == {"extract": 5, "transform": 5, "save_results": 5}
    assert result.metadata["rows"] == 2
    assert result.artifacts.write_artifacts.data_path == tmp_path / "dataset.csv"


def test_run_stops_after_retry_exhaustion(tmp_path: Path) -> None:
    executor = RecordingExecutor(max_retries=0)
    runtime = DummyRuntime(executor=executor)

    result = runtime.run(tmp_path)

    assert result.success is False
    assert result.error == "transient"
    # transform fails on first attempt and pipeline stops
    assert executor.calls == [("extract", 1), ("transform", 1)]
    assert result.rows == 0
