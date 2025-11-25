from __future__ import annotations

from pathlib import Path

import pandas as pd

from bioetl.core.pipeline.unified import UnifiedPipelineBase
from bioetl.pipelines.chembl.stage_runner import StageRunner


class DummyUnifiedPipeline(UnifiedPipelineBase):
    def __init__(self) -> None:
        super().__init__({}, run_id="run-1")
        self.calls: list[str] = []

    def extract(self) -> pd.DataFrame:
        self.calls.append("extract")
        return pd.DataFrame({"id": [1]})

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.calls.append("transform")
        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.calls.append("validate")
        return df

    def write(self, df: pd.DataFrame, output_dir: Path, *, extended: bool = False) -> Path:
        self.calls.append("write")
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "dummy.csv"
        df.to_csv(path, index=False)
        return path


def test_stage_runner_invokes_run_stage(tmp_path: Path) -> None:
    pipeline = DummyUnifiedPipeline()
    runner = StageRunner(pipeline)
    runner.register_alias("all", "run")

    result = runner.run_stage("all", output_dir=tmp_path, extended=True)

    assert result.success is True
    assert Path(tmp_path / "dummy.csv").exists()

