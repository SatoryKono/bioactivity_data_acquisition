"""Unit tests for :mod:`bioetl.core.output_writer`."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

import pandas as pd
import pytest

from bioetl.config.models import DeterminismConfig
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
    assert artifacts.metadata == tmp_path / "run-test" / "target" / "targets_meta.yaml"
    assert artifacts.qc_summary is None
    assert artifacts.qc_missing_mappings is None
    assert artifacts.qc_enrichment_metrics is None
    assert artifacts.correlation_report is None
    assert artifacts.qc_summary_statistics is None
    assert artifacts.qc_dataset_metrics is None
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


def test_atomic_writer_respects_float_precision(tmp_path):
    """Floating point serialization follows determinism float precision."""

    df = pd.DataFrame({"value": [1.23456789]})
    writer = AtomicWriter(
        "run-precision",
        determinism=DeterminismConfig(float_precision=6, datetime_format="iso8601"),
    )

    target = tmp_path / "precision.csv"
    writer.write(df, target)

    content = target.read_text(encoding="utf-8").splitlines()
    assert content[1] == "1.234568"


def test_unified_output_writer_respects_float_precision(tmp_path, monkeypatch):
    """Unified writer propagates determinism float precision to CSV outputs."""

    _freeze_datetime(monkeypatch)

    df = pd.DataFrame({"value": [1.23456789]})
    determinism = DeterminismConfig(float_precision=6, datetime_format="iso8601")
    writer = UnifiedOutputWriter("run-precision", determinism=determinism)

    output_path = tmp_path / "run-precision" / "target" / "datasets" / "targets.csv"
    writer.write(df, output_path)

    content = output_path.read_text(encoding="utf-8").splitlines()
    assert content[1] == "1.234568"


def test_unified_output_writer_writes_extended_metadata(tmp_path, monkeypatch):
    """Extended writes produce metadata with checksums and QC details."""

    _freeze_datetime(monkeypatch)

    df = pd.DataFrame(
        {"value": [10, 20, 10, 30], "category": ["x", "y", "x", "z"]}
    )
    writer = UnifiedOutputWriter("run-test")

    metadata = OutputMetadata.from_dataframe(
        df,
        pipeline_version="2.0.0",
        source_system="chembl",
        chembl_release="34",
        run_id="run-test",
        config_hash="sha256:abc",
        git_commit="deadbeefcafebabe",
        sources={
            "chembl": {
                "base_url": "https://chembl.example/api",
                "stage": "primary",
                "headers": {"Accept": "application/json"},
            }
        },
    )

    artifacts = writer.write(
        df,
        tmp_path / "run-test" / "target" / "datasets" / "targets.csv",
        extended=True,
        metadata=metadata,
    )

    assert artifacts.metadata is not None
    assert artifacts.metadata == tmp_path / "run-test" / "target" / "targets_meta.yaml"

    with artifacts.metadata.open() as fh:
        import yaml

        contents = yaml.safe_load(fh)

    assert contents["run_id"] == "run-test"
    assert contents["row_count"] == len(df)
    assert contents["column_count"] == len(df.columns)
    assert contents["column_order"] == list(df.columns)
    assert contents["config_hash"] == "sha256:abc"
    assert contents["git_commit"] == "deadbeefcafebabe"
    assert contents["sources"] == metadata.sources

    expected_checksums = {
        artifacts.dataset.name: hashlib.sha256(artifacts.dataset.read_bytes()).hexdigest(),
        artifacts.quality_report.name: hashlib.sha256(
            artifacts.quality_report.read_bytes()
        ).hexdigest(),
    }
    for extra_artifact in (
        artifacts.correlation_report,
        artifacts.qc_summary_statistics,
        artifacts.qc_dataset_metrics,
    ):
        if extra_artifact is not None:
            expected_checksums[extra_artifact.name] = hashlib.sha256(
                extra_artifact.read_bytes()
            ).hexdigest()
    assert contents["file_checksums"] == expected_checksums
    assert contents["artifacts"]["dataset"] == str(artifacts.dataset)
    assert contents["artifacts"]["quality_report"] == str(artifacts.quality_report)
    qc_artifacts = contents["artifacts"].get("qc", {})
    assert qc_artifacts["correlation_report"] == str(artifacts.correlation_report)
    assert qc_artifacts["summary_statistics"] == str(artifacts.qc_summary_statistics)
    assert qc_artifacts["dataset_metrics"] == str(artifacts.qc_dataset_metrics)

    quality_df = pd.read_csv(artifacts.quality_report)
    column_profiles = quality_df[quality_df["metric"] == "column_profile"]
    assert set(column_profiles["column"]) == set(df.columns)
    assert set(column_profiles["dtype"]) == {"int64", "object"}
    assert column_profiles["null_count"].sum() == 0

    assert artifacts.correlation_report is not None
    correlation_df = pd.read_csv(artifacts.correlation_report)
    assert set(correlation_df.columns) == {"feature_x", "feature_y", "correlation"}
    pivot = correlation_df.pivot_table(
        index="feature_x", columns="feature_y", values="correlation"
    )
    assert pytest.approx(pivot.loc["value", "value"], rel=1e-6) == 1.0

    assert artifacts.qc_summary_statistics is not None
    summary_df = pd.read_csv(artifacts.qc_summary_statistics)
    assert "column" in summary_df.columns
    assert set(summary_df["column"]) == set(df.columns)

    assert artifacts.qc_dataset_metrics is not None
    metrics_df = pd.read_csv(artifacts.qc_dataset_metrics)
    assert set(metrics_df.columns) >= {"metric", "value"}
    row_count_value = metrics_df.loc[
        metrics_df["metric"] == "row_count", "value"
    ].iloc[0]
    assert int(row_count_value) == len(df)

    duplicate_value = metrics_df.loc[
        metrics_df["metric"] == "duplicate_rows", "value"
    ].iloc[0]
    assert int(duplicate_value) == 2


def test_unified_output_writer_metadata_write_is_atomic(tmp_path, monkeypatch):
    """Metadata writes should not corrupt existing files when failures occur."""

    _freeze_datetime(monkeypatch)

    df = pd.DataFrame({"value": [1, 2, 3]})
    writer = UnifiedOutputWriter("run-atomic")

    metadata = OutputMetadata.from_dataframe(
        df,
        pipeline_version="3.0.0",
        source_system="chembl",
        chembl_release="35",
        run_id="run-atomic",
    )

    output_path = tmp_path / "run-atomic" / "target" / "datasets" / "targets.csv"
    artifacts = writer.write(
        df,
        output_path,
        extended=True,
        metadata=metadata,
    )

    metadata_path = artifacts.metadata
    assert metadata_path is not None
    baseline_contents = metadata_path.read_text(encoding="utf-8")

    import yaml as pyyaml

    def failing_dump(*args, **kwargs):  # noqa: ANN001 - signature defined by yaml.dump
        raise RuntimeError("metadata boom")

    monkeypatch.setattr(pyyaml, "dump", failing_dump)

    with pytest.raises(RuntimeError):
        writer.write(
            df,
            output_path,
            extended=True,
            metadata=metadata,
        )

    assert metadata_path.read_text(encoding="utf-8") == baseline_contents
    assert not list((tmp_path / "run-atomic").rglob("*.tmp"))


def test_calculate_checksums_supports_multi_chunk_files(tmp_path):
    """Checksum calculation should stream large files in multiple chunks."""

    writer = UnifiedOutputWriter("run-checksums")
    large_file = tmp_path / "large.bin"

    content = b"abc123" * (1024 * 1024) + b"tail"
    large_file.write_bytes(content)

    expected_checksum = hashlib.sha256(content).hexdigest()

    checksums = writer._calculate_checksums(large_file)

    assert checksums == {large_file.name: expected_checksum}


def test_unified_output_writer_emits_qc_artifacts(tmp_path, monkeypatch):
    """QC summary JSON and related CSV artefacts should be written when provided."""

    _freeze_datetime(monkeypatch)

    df = pd.DataFrame({"value": [1, 2], "label": ["x", "y"]})
    writer = UnifiedOutputWriter("run-qc")

    qc_summary = {
        "row_counts": {"total": 2, "success": 2, "fallback": 0},
        "metrics": {"duplicates": {"value": 0, "threshold": 0, "passed": True}},
    }

    qc_missing = pd.DataFrame(
        [
            {"stage": "uniprot", "target_chembl_id": "CHEMBL1", "status": "missing"},
            {"stage": "iuphar", "target_chembl_id": "CHEMBL2", "status": "fallback"},
        ]
    )

    qc_enrichment = pd.DataFrame(
        [
            {"metric": "uniprot", "value": 0.9, "threshold_min": 0.8, "passed": True},
            {"metric": "iuphar", "value": 0.4, "threshold_min": 0.6, "passed": False},
        ]
    )

    output_path = tmp_path / "run-qc" / "target" / "datasets" / "targets.csv"
    artifacts = writer.write(
        df,
        output_path,
        qc_summary=qc_summary,
        qc_missing_mappings=qc_missing,
        qc_enrichment_metrics=qc_enrichment,
    )

    assert artifacts.qc_summary == tmp_path / "run-qc" / "target" / "qc" / "qc_summary.json"
    assert artifacts.qc_missing_mappings == tmp_path / "run-qc" / "target" / "qc" / "qc_missing_mappings.csv"
    assert (
        artifacts.qc_enrichment_metrics
        == tmp_path / "run-qc" / "target" / "qc" / "qc_enrichment_metrics.csv"
    )

    with artifacts.qc_summary.open("r", encoding="utf-8") as handle:
        summary_payload = json.load(handle)

    assert summary_payload["row_counts"]["total"] == 2
    assert summary_payload["metrics"]["duplicates"]["passed"] is True

    missing_df = pd.read_csv(artifacts.qc_missing_mappings)
    assert sorted(missing_df["stage"].tolist()) == ["iuphar", "uniprot"]

    enrichment_df = pd.read_csv(artifacts.qc_enrichment_metrics)
    assert set(enrichment_df["metric"]) == {"uniprot", "iuphar"}

    with artifacts.metadata.open("r", encoding="utf-8") as handle:
        import yaml

        metadata = yaml.safe_load(handle)

    assert "qc" in metadata["artifacts"]
    qc_artifacts = metadata["artifacts"]["qc"]
    assert "correlation_report" not in qc_artifacts
    assert "summary_statistics" not in qc_artifacts
    assert "dataset_metrics" not in qc_artifacts
    assert metadata.get("qc_summary", {}).get("row_counts", {}).get("total") == 2


def test_unified_output_writer_handles_missing_optional_qc_files(tmp_path, monkeypatch):
    """Absent optional QC artefacts should not be included in metadata checksums."""

    _freeze_datetime(monkeypatch)

    df = pd.DataFrame({"value": [1, 2], "label": ["x", "y"]})
    writer = UnifiedOutputWriter("run-qc-optional")

    qc_summary = {"row_counts": {"total": 2}}

    output_path = tmp_path / "run-qc-optional" / "target" / "datasets" / "targets.csv"
    artifacts = writer.write(df, output_path, qc_summary=qc_summary)

    assert artifacts.qc_summary is not None
    assert artifacts.qc_missing_mappings is None
    assert artifacts.qc_enrichment_metrics is None

    with artifacts.metadata.open("r", encoding="utf-8") as handle:
        import yaml

        metadata = yaml.safe_load(handle)

    checksums = metadata["file_checksums"]
    assert "qc_missing_mappings.csv" not in checksums
    assert "qc_enrichment_metrics.csv" not in checksums
    assert "qc_summary.json" in checksums

    qc_artifacts = metadata["artifacts"].get("qc", {})
    assert "qc_missing_mappings" not in qc_artifacts
    assert "qc_enrichment_metrics" not in qc_artifacts
    assert qc_artifacts.get("qc_summary") == str(artifacts.qc_summary)
    assert "correlation_report" not in qc_artifacts
    assert "summary_statistics" not in qc_artifacts
    assert "dataset_metrics" not in qc_artifacts


def test_write_dataframe_json_uses_atomic_write(tmp_path, monkeypatch):
    """JSON exports rely on atomic writes and produce sorted keys."""

    _freeze_datetime(monkeypatch)

    df = pd.DataFrame({"value": [1], "label": ["b"]})
    writer = UnifiedOutputWriter("run-json")

    target = tmp_path / "run-json" / "reports" / "dataset.json"
    writer.write_dataframe_json(df, target)

    assert target.exists()
    content = target.read_text(encoding="utf-8")
    # ``sort_keys=True`` guarantees alphabetical ordering inside objects.
    assert content.index('"label"') < content.index('"value"')


def test_write_dataframe_json_cleans_up_on_failure(tmp_path, monkeypatch):
    """Temporary files from JSON writes are removed when serialization fails."""

    _freeze_datetime(monkeypatch)

    df = pd.DataFrame({"value": [1], "label": ["b"]})
    writer = UnifiedOutputWriter("run-json")

    def boom(*args, **kwargs):  # noqa: ANN001 - signature mirrors json.dump
        raise RuntimeError("boom")

    monkeypatch.setattr("bioetl.core.output_writer.json.dump", boom)

    target = tmp_path / "run-json" / "reports" / "dataset.json"

    with pytest.raises(RuntimeError):
        writer.write_dataframe_json(df, target)

    assert not target.exists(), "no JSON artefact should be present after failure"
    assert not any(tmp_path.rglob("*.tmp")), "temporary JSON files should be removed"
    assert not list(tmp_path.rglob(".tmp_run_run-json")), "run-scoped temp dirs are cleaned"


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

