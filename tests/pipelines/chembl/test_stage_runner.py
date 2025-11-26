from __future__ import annotations

from pathlib import Path

import pandas as pd

from bioetl.core.pipeline.types import (
    StageExecutionOptions,
    WriteArtifacts,
    WriteResult,
)
from bioetl.core.pipeline.unified import UnifiedPipelineBase
from bioetl.pipelines.chembl.stage_runner import run_chembl_stage


class DummyPipeline(UnifiedPipelineBase):
    pipeline_code = "dummy"

    def __init__(self) -> None:
        super().__init__({}, run_id="run-1")
        self.calls: list[str] = []

    def build_descriptor(self) -> str:
        self.calls.append("build_descriptor")
        return "descriptor"

    def prepare_run(self, options: StageExecutionOptions) -> None:
        self.calls.append("prepare_run")

    def extract(
        self,
        descriptor: str,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        self.calls.append("extract")
        return pd.DataFrame({"id": [1], "descriptor": [descriptor]})

    def transform(
        self,
        df: pd.DataFrame,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        self.calls.append("transform")
        return df.assign(transformed=True)

    def validate(
        self,
        df: pd.DataFrame,
        options: StageExecutionOptions,
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
        artifacts.data_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(artifacts.data_path, index=False)
        return WriteResult(rows=len(df), artifacts=artifacts)


def test_run_stage_executes_single_stage(tmp_path: Path) -> None:
    pipeline = DummyPipeline()

    result = run_chembl_stage(pipeline, "extract", output_dir=tmp_path)

    assert isinstance(result, pd.DataFrame)
    assert "build_descriptor" in pipeline.calls
    assert "extract" in pipeline.calls


def test_run_stage_respects_transform_context(tmp_path: Path) -> None:
    pipeline = DummyPipeline()
    df = pd.DataFrame({"id": [1]})

    transformed = run_chembl_stage(
        pipeline,
        "transform",
        df=df,
        output_dir=tmp_path,
    )

    assert "transform" in pipeline.calls
    assert list(transformed.columns) == ["id", "transformed"]


def test_run_stage_supports_write_alias(tmp_path: Path) -> None:
    pipeline = DummyPipeline()
    df = pd.DataFrame({"id": [1]})

    result = run_chembl_stage(pipeline, "write", df=df, output_dir=tmp_path)

    assert result.rows == 1
    assert result.artifacts.data_path.exists()


def test_run_stage_proxies_full_run(tmp_path: Path) -> None:
    pipeline = DummyPipeline()

    result = run_chembl_stage(pipeline, "run", output_dir=tmp_path)

    assert result.success is True
    assert result.artifacts.write_artifacts.data_path.exists()
