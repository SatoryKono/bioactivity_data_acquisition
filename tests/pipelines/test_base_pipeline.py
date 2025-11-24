"""Regression tests for the base pipeline template."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from infrastructure.config.models.models import PipelineConfig
from infrastructure.io import WriteResult
from application.pipelines import RunResult
from application.pipelines.specs.base import PipelineBase, PipelineExtractionMode


class _TemplateProbePipeline(PipelineBase):
    """Minimal concrete pipeline used to exercise the run template."""

    def __init__(self, config: PipelineConfig, run_id: str, calls: list[str]) -> None:
        super().__init__(config, run_id)
        self._calls = calls

    def prepare_run(self) -> None:
        self._calls.append("prepare")

    def extract(
        self,
        *,
        mode: PipelineExtractionMode = PipelineExtractionMode.AUTO,
        ids: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        self._calls.append("extract")
        return super().extract(mode=mode, ids=ids)

    def extract_all(self) -> pd.DataFrame:
        return pd.DataFrame({"value": [1]})

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:
        return pd.DataFrame({"value": list(range(len(ids)))})

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self._calls.append("transform")
        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        self._calls.append("validate")
        return df

    def save_results(
        self,
        df: pd.DataFrame,
        output_path: Path,
        *,
        extended: bool = False,
        include_correlation: bool | None = None,
        include_qc_metrics: bool | None = None,
    ) -> RunResult:
        self._calls.append("save")
        dataset_path = output_path / "result.csv"
        dataset_path.parent.mkdir(parents=True, exist_ok=True)
        dataset_path.write_text("value\n1\n", encoding="utf-8")
        write_result = WriteResult(dataset=dataset_path)
        return RunResult(write_result=write_result, run_directory=output_path)

    def finalize_run(self, result: RunResult | None) -> None:
        self._calls.append("finalize")


def test_run_template_order(
    tmp_path: Path,
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> None:
    calls: list[str] = []
    pipeline = _TemplateProbePipeline(pipeline_config_fixture, run_id, calls)

    pipeline.run(tmp_path)

    assert calls == ["prepare", "extract", "transform", "validate", "save", "finalize"]
