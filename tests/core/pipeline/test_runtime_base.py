from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from bioetl.core.pipeline.runtime import PipelineRuntimeBase, StagePlanExecutor
from bioetl.core.pipeline.types import (
    PipelineStageCommand,
    StageContext,
    StageExecutionOptions,
    StageRuntimeContext,
    WriteArtifacts,
)


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
        runtime: StageRuntimeContext,
        *,
        include_qc_metrics: bool,
    ) -> tuple[dict[str, int], str | None, Path | None]:
        durations: dict[str, int] = {}
        error: str | None = None
        if isinstance(runtime.input_data, pd.DataFrame):
            runtime.attributes["last_dataframe"] = runtime.input_data
        for command in stage_plan:
            attempts = 0
            while True:
                attempts += 1
                self.calls.append((command.name, attempts))
                try:
                    runtime.input_data = command.handler(context, runtime)
                    if isinstance(runtime.input_data, pd.DataFrame):
                        runtime.attributes["last_dataframe"] = runtime.input_data
                    break
                except RetryableError as exc:
                    if attempts > self.max_retries:
                        error = str(exc)
                        runtime.input_data = None
                        runtime.attributes["last_dataframe"] = None
                        break
                    continue
                except Exception as exc:  # pragma: no cover - defensive
                    error = str(exc)
                    runtime.input_data = None
                    runtime.attributes["last_dataframe"] = None
                    break
            durations[command.name] = durations.get(command.name, 0) + 5
            if error:
                break
        return durations, error, None


class DummyRuntime(PipelineRuntimeBase):
    def __init__(self, *, executor: RecordingExecutor) -> None:
        super().__init__({}, stage_plan_executor=executor)
        self.executor = executor
        self.calls: list[str] = []
        self.transform_attempts = 0

    def build_stage_plan(
        self, context: StageContext, runtime: StageRuntimeContext
    ) -> tuple[PipelineStageCommand, ...]:
        self.calls.append("build_stage_plan")

        def _extract(ctx: StageContext, exec_runtime: StageRuntimeContext) -> pd.DataFrame:
            self.calls.append("extract")
            exec_runtime.attributes.setdefault("metadata", {})["extract"] = True
            return pd.DataFrame({"value": [1, 2]})

        def _transform(ctx: StageContext, exec_runtime: StageRuntimeContext) -> pd.DataFrame:
            self.calls.append("transform")
            # fail first attempt and succeed on retry
            self.transform_attempts += 1
            if self.transform_attempts == 1:
                raise RetryableError("transient")
            assert exec_runtime.input_data is not None
            return exec_runtime.input_data.assign(value=lambda s: s["value"] * 2)

        def _save(ctx: StageContext, exec_runtime: StageRuntimeContext) -> WriteArtifacts:
            self.calls.append("save_results")
            artifacts = exec_runtime.attributes.get("artifacts") or WriteArtifacts()
            artifacts.data_path = exec_runtime.attributes.get("output_dir", Path.cwd()) / "dataset.csv"
            exec_runtime.attributes["artifacts"] = artifacts
            return artifacts

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
