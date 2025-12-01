"""Test orchestration common functionality."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from bioetl.core.pipeline.unified import UnifiedPipelineBase
from bioetl.core.pipeline.services import QCService
from bioetl.core.io.artifacts import WriteArtifacts
from bioetl.core.pipeline.types import (
    MaterializationConfig,
    PipelineConfig,
    PipelineInfo,
    RunResult,
    StageExecutionOptions,
    WriteResult,
)


class QCPipeline(UnifiedPipelineBase):
    """Test pipeline for QC functionality."""

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        self.validator = None
        super().__init__(config, run_id=run_id)
        self._finalized = False

    def prepare_run(
        self,
        options: StageExecutionOptions,
    ) -> None:  # pragma: no cover - not used
        return None

    def extract(
        self, descriptor: object, options: StageExecutionOptions
    ) -> pd.DataFrame:
        return pd.DataFrame({"id": [1, 2], "value": [1, 2]})

    def transform(
        self, df: pd.DataFrame, options: StageExecutionOptions
    ) -> pd.DataFrame:
        return df

    def validate(
        self, df: pd.DataFrame, options: StageExecutionOptions
    ) -> pd.DataFrame:
        return df

    def save_results(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        options: StageExecutionOptions,
    ) -> WriteResult:
        artifacts.data_path = artifacts.data_path or (
            self.output_root / "data.csv"
        )
        artifacts.data_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(artifacts.data_path, index=False)
        return WriteResult(rows=int(df.shape[0]), artifacts=artifacts)

    def finalize_run(self, run_result: RunResult) -> None:
        """Finalize the pipeline run."""
        self._finalized = True


def test_pipeline_runs_qc_when_requested(tmp_path: Path) -> None:
    """Test that pipeline runs QC when requested."""
    config = PipelineConfig(
        pipeline=PipelineInfo(name="qc"),
        materialization=MaterializationConfig(root=tmp_path / "out"),
    )
    pipeline = QCPipeline(config, run_id="qc-test")

    result = pipeline.run(tmp_path / "output", include_qc_metrics=True)

    assert result.success is True
    assert result.artifacts.qc_metrics_path is not None
    assert result.artifacts.qc_metrics_path.exists()
    assert pipeline._finalized is True


def test_pipeline_skips_qc_when_not_requested(tmp_path: Path) -> None:
    """Test that pipeline skips QC when not requested."""
    config = PipelineConfig(
        pipeline=PipelineInfo(name="qc"),
        materialization=MaterializationConfig(root=tmp_path / "out"),
    )
    pipeline = QCPipeline(config, run_id="qc-test")

    result = pipeline.run(tmp_path / "output", include_qc_metrics=False)

    assert result.success is True
    assert result.artifacts.qc_metrics_path is None
    assert pipeline._finalized is True


def test_pipeline_skips_qc_when_service_disabled(tmp_path: Path) -> None:
    """Test that pipeline skips QC when service is disabled."""
    config = PipelineConfig(
        pipeline=PipelineInfo(name="qc"),
        materialization=MaterializationConfig(root=tmp_path / "out"),
    )
    pipeline = QCPipeline(config, run_id="qc-test")
    pipeline.qc_service = QCService(enabled=False)

    result = pipeline.run(tmp_path / "output", include_qc_metrics=True)

    assert result.success is True
    assert result.artifacts.qc_metrics_path is None
    assert pipeline._finalized is True  # noqa: SLF001