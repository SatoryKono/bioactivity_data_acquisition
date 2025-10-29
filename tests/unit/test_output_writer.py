"""Unit tests for :mod:`bioetl.core.output_writer`."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

import pandas as pd
import pytest

from bioetl.core.output_writer import AtomicWriter, OutputMetadata, UnifiedOutputWriter


class _FixedDateTime:
    """Helper class used to freeze ``datetime.now`` within the module under test."""

    fixed = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz: timezone | None = None) -> datetime:
        return cls.fixed if tz is None else cls.fixed.astimezone(tz)


def _freeze_datetime(monkeypatch: pytest.MonkeyPatch) -> None:
    """Freeze ``datetime`` inside :mod:`bioetl.core.output_writer`."""

    monkeypatch.setattr("bioetl.core.output_writer.datetime", _FixedDateTime)


def test_unified_output_writer_writes_deterministic_outputs(tmp_path, monkeypatch):
    """Dataset and QC outputs should use deterministic naming and content."""

    _freeze_datetime(monkeypatch)

    df = pd.DataFrame({"value": [1, 2], "label": ["a", "b"]})
    writer = UnifiedOutputWriter("run-test")

    output_path = tmp_path / "run-test" / "target" / "datasets" / "targets.csv"
    artifacts = writer.write(df, output_path)

    assert artifacts.dataset == output_path
    assert (
        artifacts.quality_report
        == tmp_path / "run-test" / "target" / "qc" / "targets_quality_report.csv"
    )
    assert artifacts.run_directory == tmp_path / "run-test" / "target"
    assert artifacts.metadata == tmp_path / "run-test" / "target" / "meta.yaml"
    assert artifacts.qc_summary is None
    assert artifacts.qc_missing_mappings is None
    assert artifacts.qc_enrichment_metrics is None
    assert artifacts.debug_dataset is None

    written_df = pd.read_csv(artifacts.dataset)
    pd.testing.assert_frame_equal(written_df, df)

    quality_df = pd.read_csv(artifacts.quality_report)
    assert set(quality_df.columns) >= {
        "metric",
        "column",
        "null_count",
        "null_fraction",
        "unique_count",
        "dtype",
    }
    assert (quality_df["metric"] == "column_profile").all()
    assert set(quality_df["column"]) == set(df.columns)


def test_unified_output_writer_cleans_up_on_failure(tmp_path, monkeypatch):
    """Temporary files and directories are cleaned when a write fails."""

    _freeze_datetime(monkeypatch)

    def boom(*args, **kwargs):  # noqa: ANN001, D401 - signature dictated by patch target
        raise RuntimeError("boom")

    monkeypatch.setattr(AtomicWriter, "_write_to_file", boom)

    writer = UnifiedOutputWriter("run-test")

    with pytest.raises(RuntimeError):
        writer.write(
            pd.DataFrame({"value": [1]}),
            tmp_path / "run-test" / "target" / "datasets" / "targets.csv",
        )

    tmp_dirs = list(tmp_path.glob(".tmp_run_run-test"))
    tmp_files = list(tmp_path.rglob("*.tmp"))

    assert not tmp_dirs, "temporary run directory should be removed"
    assert not tmp_files, "temporary files should be removed"
    assert not any(tmp_path.rglob("targets.csv")), "no dataset files should remain"


def test_unified_output_writer_writes_extended_metadata(tmp_path, monkeypatch):
    """Extended writes produce metadata with checksums and QC details."""

    _freeze_datetime(monkeypatch)

    df = pd.DataFrame({"value": [10, 20, 30], "category": ["x", "y", "z"]})
    writer = UnifiedOutputWriter("run-test")

    metadata = OutputMetadata.from_dataframe(
        df,
        pipeline_version="2.0.0",
        source_system="chembl",
        chembl_release="34",
        run_id="run-test",
    )

    artifacts = writer.write(
        df,
        tmp_path / "run-test" / "target" / "datasets" / "targets.csv",
        extended=True,
        metadata=metadata,
    )

    assert artifacts.metadata is not None
    assert artifacts.metadata == tmp_path / "run-test" / "target" / "meta.yaml"

    with artifacts.metadata.open() as fh:
        import yaml

        contents = yaml.safe_load(fh)

    assert contents["run_id"] == "run-test"
    assert contents["row_count"] == len(df)
    assert contents["column_count"] == len(df.columns)
    assert contents["column_order"] == list(df.columns)

    expected_checksums = {
        artifacts.dataset.name: hashlib.sha256(artifacts.dataset.read_bytes()).hexdigest(),
        artifacts.quality_report.name: hashlib.sha256(
            artifacts.quality_report.read_bytes()
        ).hexdigest(),
    }
    assert contents["file_checksums"] == expected_checksums
    assert contents["artifacts"]["dataset"] == str(artifacts.dataset)
    assert contents["artifacts"]["quality_report"] == str(artifacts.quality_report)
    assert "qc" not in contents.get("artifacts", {})

    quality_df = pd.read_csv(artifacts.quality_report)
    column_profiles = quality_df[quality_df["metric"] == "column_profile"]
    assert set(column_profiles["column"]) == set(df.columns)
    assert set(column_profiles["dtype"]) == {"int64", "object"}
    assert column_profiles["null_count"].sum() == 0


def test_unified_output_writer_writes_qc_artifacts(tmp_path, monkeypatch):
    """QC summary JSON and CSV artefacts should be persisted when provided."""

    _freeze_datetime(monkeypatch)

    df = pd.DataFrame({"value": [1, 2], "label": ["a", "b"]})
    qc_summary = {
        "row_counts": {"total": 2, "fallback": 1},
        "metrics": {"coverage": 0.5},
    }
    missing_mappings = pd.DataFrame(
        [
            {
                "stage": "uniprot",
                "target_chembl_id": "CHEMBL1",
                "input_accession": "P12345",
                "resolved_accession": pd.NA,
                "resolution": "unresolved",
                "status": "missing",
            }
        ]
    )
    enrichment_metrics = pd.DataFrame(
        [
            {
                "metric": "uniprot",
                "value": 0.9,
                "threshold_min": 0.8,
                "passed": True,
                "severity": "info",
            }
        ]
    )

    writer = UnifiedOutputWriter("run-qc")
    output_path = tmp_path / "run-qc" / "target" / "datasets" / "targets.csv"

    artifacts = writer.write(
        df,
        output_path,
        qc_summary=qc_summary,
        qc_missing_mappings=missing_mappings,
        qc_enrichment_metrics=enrichment_metrics,
    )

    assert artifacts.qc_summary is not None and artifacts.qc_summary.exists()
    assert artifacts.qc_missing_mappings is not None and artifacts.qc_missing_mappings.exists()
    assert (
        artifacts.qc_enrichment_metrics is not None
        and artifacts.qc_enrichment_metrics.exists()
    )

    with artifacts.qc_summary.open() as handle:
        summary_payload = json.load(handle)

    assert summary_payload["row_counts"]["total"] == 2
    assert summary_payload["metrics"]["coverage"] == 0.5

    missing_df = pd.read_csv(artifacts.qc_missing_mappings)
    assert missing_df.iloc[0]["stage"] == "uniprot"
    assert missing_df.iloc[0]["status"] == "missing"

    enrichment_df = pd.read_csv(artifacts.qc_enrichment_metrics)
    assert enrichment_df.iloc[0]["metric"] == "uniprot"
    assert bool(enrichment_df.iloc[0]["passed"]) is True

    meta_path = artifacts.metadata
    with meta_path.open() as fh:
        import yaml

        meta = yaml.safe_load(fh)

    assert "qc_summary" in meta
    assert meta["qc_summary"]["row_counts"]["total"] == 2
    qc_artifacts = meta.get("artifacts", {}).get("qc", {})
    assert "qc_summary" in qc_artifacts
    assert "qc_missing_mappings" in qc_artifacts
    assert "qc_enrichment_metrics" in qc_artifacts


def test_write_dataframe_json(tmp_path, monkeypatch):
    """Debug dataframe JSON writer should persist records atomically."""

    _freeze_datetime(monkeypatch)

    df = pd.DataFrame(
        {
            "value": [1, 2],
            "label": ["a", "b"],
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-02"]),
        }
    )
    writer = UnifiedOutputWriter("run-json")

    json_path = tmp_path / "run-json" / "activity" / "datasets" / "activity.json"
    writer.write_dataframe_json(df, json_path)

    assert json_path.exists()

    with json_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    assert isinstance(payload, list)
    assert payload[0]["value"] == 1
    assert payload[0]["label"] == "a"
    assert payload[0]["timestamp"].startswith("2024-01-01")

