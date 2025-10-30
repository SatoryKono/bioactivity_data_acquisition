from __future__ import annotations

import types
from pathlib import Path

import pandas as pd

from bioetl.pipelines.base import PipelineBase


class _ExtendedArtifactsPipeline(PipelineBase):
    """Minimal pipeline used to exercise extended artefact generation."""

    def extract(self, *args, **kwargs):  # type: ignore[override]
        return pd.DataFrame(
            {
                "numeric_a": [1, 2, 3, 4],
                "numeric_b": [4, 3, 2, 1],
                "category": ["a", "b", "c", "d"],
            }
        )

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # type: ignore[override]
        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:  # type: ignore[override]
        return df


def _make_config() -> types.SimpleNamespace:
    pipeline_section = types.SimpleNamespace(name="integration", entity="integration")
    qc_section = types.SimpleNamespace(severity_threshold="error")
    return types.SimpleNamespace(pipeline=pipeline_section, qc=qc_section, cli={})


def test_pipeline_run_emits_extended_artifacts(tmp_path: Path) -> None:
    """Running with ``extended=True`` should persist correlation and QC artefacts."""

    config = _make_config()
    pipeline = _ExtendedArtifactsPipeline(config, run_id="integration-test")

    output_path = tmp_path / "integration" / "datasets" / "integration.csv"
    artifacts = pipeline.run(output_path, extended=True)

    assert artifacts.correlation_report is not None
    assert artifacts.correlation_report.exists()
    assert artifacts.qc_summary_statistics is not None
    assert artifacts.qc_summary_statistics.exists()
    assert artifacts.qc_dataset_metrics is not None
    assert artifacts.qc_dataset_metrics.exists()

    correlation_df = pd.read_csv(artifacts.correlation_report)
    assert not correlation_df.empty
    assert set(correlation_df.columns) == {"feature_x", "feature_y", "correlation"}

    summary_df = pd.read_csv(artifacts.qc_summary_statistics)
    assert "column" in summary_df.columns
    assert set(summary_df["column"]) >= {"numeric_a", "numeric_b", "category"}

    metrics_df = pd.read_csv(artifacts.qc_dataset_metrics)
    assert {"metric", "value"}.issubset(metrics_df.columns)
    assert "row_count" in metrics_df["metric"].values

    meta_path = artifacts.metadata
    assert meta_path is not None and meta_path.exists()
    metadata = meta_path.read_text(encoding="utf-8")
    assert "correlation_report" in metadata
    assert "summary_statistics" in metadata
    assert "dataset_metrics" in metadata
