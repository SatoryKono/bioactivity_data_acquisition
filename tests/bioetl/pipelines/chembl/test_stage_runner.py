"""Unit tests covering the stage runner helpers for ChEMBL pipelines."""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from bioetl.core.pipeline import PipelineBase
from bioetl.pipelines.chembl.stage_runner import (
    StageContext,
    build_stage_functions,
)


class _StubPipeline(PipelineBase):
    """Minimal pipeline implementation for stage runner tests."""

    actor = "stub_pipeline"
    id_column = "stub_id"

    def _result(self, stage: str, args: tuple[object, ...], kwargs: dict[str, object]) -> dict[str, object]:
        return {
            "stage": stage,
            "args": args,
            "kwargs": kwargs,
            "run_id": self.run_id,
            "pipeline": self.config.pipeline.name,
        }

    def extract(self, *args: object, **kwargs: object) -> dict[str, object]:
        return self._result("extract", args, kwargs)

    def extract_all(self) -> dict[str, object]:
        return self._result("extract_all", tuple(), {})

    def extract_by_ids(self, ids: Sequence[str]) -> dict[str, object]:  # pragma: no cover - exercised via protocol checks
        return self._result("extract_by_ids", (tuple(ids),), {})

    def transform(self, df: object) -> dict[str, object]:  # pragma: no cover - exercised via protocol checks
        return self._result("transform", (df,), {})


def test_stage_function_invocation_with_config(pipeline_config_fixture, run_id: str) -> None:
    _, stages = build_stage_functions(_StubPipeline, stages=("extract",))
    result = stages["extract"](pipeline_config_fixture, run_id, "payload")

    assert result["stage"] == "extract"
    assert result["run_id"] == run_id
    assert result["args"] == ("payload",)


def test_stage_function_accepts_stage_context(pipeline_config_fixture) -> None:
    _, stages = build_stage_functions(_StubPipeline, stages=("extract",))
    context = StageContext(
        config=pipeline_config_fixture,
        run_id="context-run",
        pipeline_name="cli-stage",
    )

    result = stages["extract"](context, "payload")

    assert result["run_id"] == "context-run"
    assert result["pipeline"] == "cli-stage"


def test_stage_function_rejects_missing_run_id(pipeline_config_fixture) -> None:
    _, stages = build_stage_functions(_StubPipeline, stages=("extract",))
    with pytest.raises(TypeError):
        stages["extract"](pipeline_config_fixture)


def test_stage_function_rejects_redundant_run_id(pipeline_config_fixture) -> None:
    _, stages = build_stage_functions(_StubPipeline, stages=("extract",))
    context = StageContext(config=pipeline_config_fixture, run_id="ctx")

    with pytest.raises(TypeError):
        stages["extract"](context, "duplicate-run-id")
