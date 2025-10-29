"""Unit tests for :mod:`bioetl.core.output_writer`."""

from __future__ import annotations

import hashlib
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

