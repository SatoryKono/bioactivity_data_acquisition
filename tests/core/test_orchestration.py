from __future__ import annotations

from typing import Any
from pathlib import Path

import pandas as pd
import pytest

from bioetl.core.pipeline.unified import UnifiedPipelineBase
from bioetl.core.io.artifacts import WriteArtifacts
from bioetl.core.pipeline.types import (
    MaterializationConfig,
    PipelineConfig,
    PipelineInfo,
    PipelineBaseProtocol,
    RunResult,
    StageExecutionOptions,
    WriteResult,
)


class DummyPipeline(UnifiedPipelineBase):
    """Simple pipeline used to validate the lifecycle contract."""

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id=run_id)
        self.calls: list[str] = []
        self.finalized: RunResult | None = None

    def prepare_run(self, options: StageExecutionOptions) -> None:
        self.calls.append("prepare_run")

    def extract(
        self, descriptor: Any, options: StageExecutionOptions
    ) -> pd.DataFrame:
        self.calls.append("extract")
        return pd.DataFrame({"value": [1, 2, 3]})

    def transform(
        self, df: pd.DataFrame, options: StageExecutionOptions
    ) -> pd.DataFrame:
        self.calls.append("transform")
        return df.assign(value=df["value"] * 2)

    def validate(
        self, df: pd.DataFrame, options: StageExecutionOptions
    ) -> pd.DataFrame:
        self.calls.append("validate")
        return df

    def save_results(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        options: StageExecutionOptions,
    ) -> WriteResult:
        self.calls.append("save_results")
        artifacts.data_path = (
            artifacts.data_path or self.output_root / "result.csv"
        )
        artifacts.data_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(artifacts.data_path, index=False)
        return WriteResult(rows=int(df.shape[0]), artifacts=artifacts)

    def finalize_run(self, run_result: RunResult) -> None:
        self.calls.append("finalize_run")
        self.finalized = run_result


@pytest.fixture
def pipeline(tmp_path: Path) -> DummyPipeline:
    config = PipelineConfig(
        pipeline=PipelineInfo(name="dummy"),
        materialization=MaterializationConfig(root=tmp_path / "out"),
    )
    return DummyPipeline(config, run_id="test-run")


def test_pipeline_run_executes_stages(
    pipeline: DummyPipeline,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    result = pipeline.run(output_dir, run_tag="nightly", mode="full")

    assert result.success
    assert result.rows == 3
    assert [
        "prepare_run",
        "extract",
        "transform",
        "validate",
        "save_results",
        "finalize_run",
    ] == pipeline.calls
    assert result.artifacts.write_artifacts is not None
    assert result.artifacts.write_artifacts.data_path is not None
    assert result.artifacts.write_artifacts.data_path.exists()
    assert result.metadata["stage_plan"] == [
        "extract",
        "transform",
        "validate",
        "save_results",
    ]


def test_dry_run_skips_writing(
    pipeline: DummyPipeline,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    result = pipeline.run(output_dir, dry_run=True)

    assert result.success
    assert "save_results" not in pipeline.calls
    assert result.artifacts.write_artifacts is not None
    data_path = result.artifacts.write_artifacts.data_path
    assert data_path is not None
    assert not data_path.exists()


def test_pipeline_adheres_to_base_protocol(pipeline: DummyPipeline) -> None:
    assert isinstance(pipeline, PipelineBaseProtocol)


def test_pipeline_sample_option_limits_rows(
    pipeline: DummyPipeline,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    result = pipeline.run(output_dir, sample=1)

    assert result.rows == 1
