from __future__ import annotations

import json
import types
from pathlib import Path

import pandas as pd
import pytest
import yaml

from bioetl.pandera_pandas import DataFrameModel
from bioetl.pandera_typing import Series
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas.registry import schema_registry


pytestmark = pytest.mark.integration


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
        # Mimic production pipelines by materialising export metadata during
        # transformation so ``UnifiedOutputWriter`` receives a fully populated
        # contract.
        self.set_export_metadata_from_dataframe(
            df,
            pipeline_version=self.config.pipeline.version,
            source_system=self.config.pipeline.entity,
            chembl_release=None,
        )
        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:  # type: ignore[override]
        return df

    def close_resources(self) -> None:  # type: ignore[override]
        """No-op resource cleanup for the test pipeline."""
        return None


@pytest.fixture
def integration_schema_registration() -> type[DataFrameModel]:
    """Register a minimal schema for integration order checks."""

    class IntegrationSchema(DataFrameModel):
        numeric_a: Series[int]
        numeric_b: Series[int]
        category: Series[str]

        @classmethod
        def get_column_order(cls) -> list[str]:
            return ["numeric_a", "numeric_b", "category"]

        _column_order = ["numeric_a", "numeric_b", "category"]

    entity = "integration.order-check"
    version = "1.0.0"
    schema_registry.register(entity, version, IntegrationSchema)

    try:
        yield IntegrationSchema
    finally:
        registry_entry = schema_registry._registry.get(entity)
        if registry_entry is not None:
            registry_entry.pop(version, None)
            if not registry_entry:
                schema_registry._registry.pop(entity, None)
        schema_registry._metadata.pop((entity, version), None)


def _make_config() -> types.SimpleNamespace:
    pipeline_section = types.SimpleNamespace(
        name="integration",
        entity="integration",
        version="1.2.3",
    )
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


@pytest.mark.integration
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

    assert metadata["run_id"] == "integration-test"
    assert metadata["pipeline_version"] == config.pipeline.version
    assert metadata["source_system"] == config.pipeline.entity
    assert metadata["chembl_release"] is None
    assert metadata["config_hash"] == config.config_hash
    assert metadata.get("git_commit") is None
    assert metadata["sources"] == ["chembl"]
    assert metadata["schema_id"] is None
    assert metadata["schema_version"] is None
    assert metadata["column_order"] == ["numeric_a", "numeric_b", "category"]
    assert metadata["column_count"] == 3
    assert metadata["row_count"] == 4
    assert metadata["file_checksums"]
    timestamp = metadata["extraction_timestamp"]
    assert timestamp.endswith("Z") or timestamp.endswith("+00:00")
    assert metadata["artifacts"]["dataset"] == str(artifacts.dataset)
    assert metadata["artifacts"]["quality_report"] == str(artifacts.quality_report)

    qc_artifacts = metadata["artifacts"].get("qc", {})
    assert "correlation_report" in qc_artifacts
    assert "summary_statistics" in qc_artifacts
    assert "dataset_metrics" in qc_artifacts


@pytest.mark.integration
def test_pipeline_run_fails_on_schema_registry_order_mismatch(
    tmp_path: Path, integration_schema_registration: type[DataFrameModel]
) -> None:
    """Pipelines with registered schemas should fail-fast on column order drift."""

    config = _make_config()

    class _OrderSensitivePipeline(_ExtendedArtifactsPipeline):
        def __init__(self, config, run_id):
            super().__init__(config, run_id)
            self.primary_schema = integration_schema_registration

        def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # type: ignore[override]
            df = super().transform(df)
            return df[["category", "numeric_a", "numeric_b"]]

    pipeline = _OrderSensitivePipeline(config, run_id="integration-order-mismatch")
    output_path = tmp_path / "integration" / "datasets" / "integration.csv"

    with pytest.raises(ValueError, match="columns do not match"):
        pipeline.run(output_path)
    manifest_path = artifacts.manifest
    assert manifest_path is not None and manifest_path.exists()

    with manifest_path.open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)

    assert manifest["run_id"] == "integration-test"
    assert manifest["artifacts"]["dataset"] == str(artifacts.dataset)
    assert manifest["artifacts"]["quality_report"] == str(artifacts.quality_report)
    assert manifest["artifacts"]["metadata"] == str(meta_path)
    assert manifest["artifacts"]["qc"]["correlation_report"] == str(
        artifacts.correlation_report
    )
    assert manifest["checksums"] == metadata["file_checksums"]
    assert manifest["schema"] == {"id": None, "version": None}
