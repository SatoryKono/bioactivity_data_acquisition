"""Unit tests covering the stage runner helpers for ChEMBL pipelines."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd
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
            "pipeline": self.pipeline_code,
        }

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:  # type: ignore[override]
        result = self._result("extract", args, kwargs)
        return pd.DataFrame([result])

    def extract_all(self) -> pd.DataFrame:  # type: ignore[override]
        result = self._result("extract_all", (), {})
        return pd.DataFrame([result])

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:  # type: ignore[override]  # pragma: no cover - exercised via protocol checks
        result = self._result("extract_by_ids", (tuple(ids),), {})
        return pd.DataFrame([result])

    def transform(self, df: object) -> pd.DataFrame:  # type: ignore[override]  # pragma: no cover - exercised via protocol checks
        result = self._result("transform", (df,), {})
        return pd.DataFrame([result])


def test_stage_function_invocation_with_config(pipeline_config_fixture, run_id: str) -> None:
    _, stages = build_stage_functions(_StubPipeline, stages=("extract",))
    df_result = stages["extract"](pipeline_config_fixture, run_id, "payload")
    result = df_result.iloc[0].to_dict()

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

    df_result = stages["extract"](context, "payload")
    result = df_result.iloc[0].to_dict()

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
        stages["extract"](context, run_id="duplicate-run-id")
