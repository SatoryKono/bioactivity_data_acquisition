from __future__ import annotations

import types
from pathlib import Path

import pandas as pd
import yaml

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
    determinism_section = types.SimpleNamespace(
        column_order=[],
        float_precision=6,
        datetime_format="iso8601",
        sort=types.SimpleNamespace(by=[], ascending=[]),
    )
    sources = {
        "chembl": types.SimpleNamespace(enabled=True),
        "pubchem": types.SimpleNamespace(enabled=False),
    }
    return types.SimpleNamespace(
        pipeline=pipeline_section,
        qc=qc_section,
        cli={},
        determinism=determinism_section,
        config_hash="integration-hash",
        sources=sources,
    )


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
    with meta_path.open("r", encoding="utf-8") as handle:
        metadata = yaml.safe_load(handle)

    assert metadata["config_hash"] == config.config_hash
    assert metadata.get("git_commit") is None
    assert metadata["sources"] == ["chembl"]
    assert "generated_at" in metadata
    assert "extraction_timestamp" not in metadata

    qc_artifacts = metadata["artifacts"].get("qc", {})
    assert "correlation_report" in qc_artifacts
    assert "summary_statistics" in qc_artifacts
    assert "dataset_metrics" in qc_artifacts
