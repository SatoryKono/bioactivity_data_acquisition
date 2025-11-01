"""Unit tests for :mod:`bioetl.core.output_writer`."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest
import yaml

from bioetl.config.loader import load_config
from bioetl.config.models import DeterminismConfig
from bioetl.config.paths import get_config_path
from bioetl.core.output_writer import (
    AdditionalTableSpec,
    AtomicWriter,
    OutputMetadata,
    UnifiedOutputWriter,
    hash_business_key,
    hash_row,
)
from bioetl.pandera_pandas import DataFrameModel
from bioetl.pandera_typing import Series
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas.base import BaseSchema
from bioetl.schemas.registry import schema_registry


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


def test_unified_output_writer_writes_parquet_outputs(tmp_path, monkeypatch):
    """Parquet datasets should include QC reports and metadata."""

    pytest.importorskip("pyarrow")

    _freeze_datetime(monkeypatch)

    df = pd.DataFrame({"value": [1, 2], "label": ["a", "b"]}).convert_dtypes()
    writer = UnifiedOutputWriter("run-parquet")

    output_path = tmp_path / "run-parquet" / "target" / "datasets" / "targets.parquet"
    artifacts = writer.write(df, output_path)

    assert artifacts.dataset == output_path
    assert artifacts.dataset.suffix == ".parquet"
    assert artifacts.quality_report.suffix == ".csv"
    assert artifacts.metadata is not None and artifacts.metadata.exists()

    parquet_df = pd.read_parquet(artifacts.dataset).convert_dtypes()
    pd.testing.assert_frame_equal(parquet_df, df)

    quality_df = pd.read_csv(artifacts.quality_report)
    assert not quality_df.empty

    with artifacts.metadata.open(encoding="utf-8") as handle:
        metadata = yaml.safe_load(handle)

    assert metadata["artifacts"]["dataset"] == str(artifacts.dataset)
    assert metadata["artifacts"]["quality_report"] == str(artifacts.quality_report)
    assert artifacts.dataset.name in metadata["file_checksums"]
    assert artifacts.quality_report.name in metadata["file_checksums"]


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
    config_path = get_config_path("base.yaml")
    writer = UnifiedOutputWriter(
        "run-test",
        pipeline_config=SimpleNamespace(source_path=config_path),
    )

    metadata = OutputMetadata.from_dataframe(
        df,
        pipeline_version="2.0.0",
        source_system="chembl",
        chembl_release="34",
        run_id="run-test",
        config_hash="sha256:deadbeef",
        git_commit="abc123def",
        sources=["chembl", "pubchem"],
        hash_policy_version="2024.01",
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
        contents = yaml.safe_load(fh)

    assert contents["run_id"] == "run-test"
    assert contents["row_count"] == len(df)
    assert contents["column_count"] == len(df.columns)
    assert contents["column_order"] == list(df.columns)
    assert contents["config_hash"] == "sha256:deadbeef"
    assert contents["git_commit"] == "abc123def"
    assert contents["sources"] == ["chembl", "pubchem"]
    assert contents["hash_policy_version"] == "2024.01"
    snapshot = contents.get("config_snapshot")
    assert snapshot is not None
    expected_config_digest = hashlib.sha256(config_path.read_bytes()).hexdigest()
    assert snapshot["path"] == "configs/base.yaml"
    assert snapshot["sha256"] == f"sha256:{expected_config_digest}"
    assert contents["lineage"] == {"source_files": [], "transformations": []}
    assert contents["checksum_algorithm"] == "sha256"

    quantitative_metrics = contents["quantitative_metrics"]
    assert quantitative_metrics["row_count"] == len(df)
    assert "duplicate_rows" in quantitative_metrics

    stage_durations = contents["stage_durations"]
    assert stage_durations["load"] >= 0

    assert contents["sort_keys"] == []
    assert contents["sort_directions"] == []

    pii_policy = contents["pii_secrets_policy"]
    assert pii_policy["pii_expected"] is False
    assert "secret_management" in pii_policy

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
    assert contents["schema_id"] is None
    assert contents["schema_version"] is None
    assert contents["column_order_source"] == "dataframe"
    assert contents.get("na_policy") == "allow"
    assert contents.get("precision_policy") == "%.6f"


def test_additional_tables_written_in_multiple_formats(tmp_path, monkeypatch):
    """Additional tables can be materialised as CSV and Parquet with checksums."""

    pytest.importorskip("pyarrow")

    _freeze_datetime(monkeypatch)

    df = pd.DataFrame({"value": [1]}).convert_dtypes()
    supplemental = pd.DataFrame({"id": [1], "flag": ["y"]}).convert_dtypes()

    writer = UnifiedOutputWriter("run-additional")
    additional_tables = {
        "supplemental": AdditionalTableSpec(
            dataframe=supplemental,
            relative_path=Path("supplemental.csv"),
            formats=("csv", "parquet"),
        )
    }

    output_path = tmp_path / "run-additional" / "target" / "datasets" / "targets.csv"
    artifacts = writer.write(
        df,
        output_path,
        additional_tables=additional_tables,
    )

    supplemental_artifacts = artifacts.additional_datasets.get("supplemental")
    assert isinstance(supplemental_artifacts, dict)

    csv_path = supplemental_artifacts.get("csv")
    parquet_path = supplemental_artifacts.get("parquet")
    assert csv_path is not None
    assert parquet_path is not None

    csv_df = pd.read_csv(csv_path).convert_dtypes()
    parquet_df = pd.read_parquet(parquet_path).convert_dtypes()
    pd.testing.assert_frame_equal(csv_df, supplemental)
    pd.testing.assert_frame_equal(parquet_df, supplemental)

    with artifacts.metadata.open(encoding="utf-8") as handle:
        metadata = yaml.safe_load(handle)

    checksums = metadata["file_checksums"]
    assert Path(csv_path).name in checksums
    assert Path(parquet_path).name in checksums


def test_unified_output_writer_includes_hash_summary(tmp_path, monkeypatch):
    """Metadata and manifest should include hash summaries when present."""

    _freeze_datetime(monkeypatch)

    df = pd.DataFrame(
        {
            "compound_id": ["CHEMBL1", "CHEMBL2"],
            "result_value": [1.0, 2.5],
        }
    )
    df["hash_business_key"] = df["compound_id"].apply(hash_business_key)
    df["hash_row"] = [
        hash_row({"compound_id": row.compound_id, "result_value": row.result_value})
        for row in df.itertuples()
    ]

    writer = UnifiedOutputWriter("run-hash-summary")
    output_path = (
        tmp_path
        / "run-hash-summary"
        / "hash"
        / "datasets"
        / "hash_summary.csv"
    )

    artifacts = writer.write(df, output_path, extended=True)

    assert artifacts.hash_summary is not None
    assert set(artifacts.hash_summary) == {"hash_row", "hash_business_key"}

    def _expected_summary(series: pd.Series) -> dict[str, Any]:
        values = [str(value) for value in series if pd.notna(value)]
        digest = hashlib.sha256()
        for value in values:
            digest.update(value.encode("utf-8"))
            digest.update(b"\n")
        return {
            "count": len(values),
            "unique": len(set(values)),
            "null_count": int(series.isna().sum()),
            "sha256": digest.hexdigest(),
        }

    expected_hash_row = _expected_summary(df["hash_row"])
    expected_hash_bk = _expected_summary(df["hash_business_key"])

    assert artifacts.hash_summary["hash_row"] == expected_hash_row
    assert artifacts.hash_summary["hash_business_key"] == expected_hash_bk

    assert artifacts.metadata is not None and artifacts.metadata.exists()
    with artifacts.metadata.open("r", encoding="utf-8") as handle:
        metadata = yaml.safe_load(handle)

    assert metadata["hash_summary"]["hash_row"] == expected_hash_row
    assert metadata["hash_summary"]["hash_business_key"] == expected_hash_bk

    assert artifacts.manifest is not None and artifacts.manifest.exists()
    manifest = json.loads(artifacts.manifest.read_text(encoding="utf-8"))
    assert manifest["hash_summary"]["hash_row"] == expected_hash_row
    assert manifest["hash_summary"]["hash_business_key"] == expected_hash_bk


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


def test_pipeline_export_metadata_receives_checksums(tmp_path, monkeypatch):
    """Pipeline export should persist checksum metadata on the pipeline instance."""

    _freeze_datetime(monkeypatch)

    class _StubPipeline(PipelineBase):
        def extract(self, *args, **kwargs):  # noqa: D401, ANN001 - abstract implementation for tests
            raise NotImplementedError

        def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - test stub
            return df

        def validate(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - test stub
            return df

        def close_resources(self) -> None:  # noqa: D401 - test stub
            return None

    config = load_config("configs/pipelines/assay.yaml")
    pipeline = _StubPipeline(config, "run-pipeline-meta")

    df = pd.DataFrame({"value": [1, 2], "label": ["a", "b"]})
    pipeline.set_export_metadata_from_dataframe(
        df,
        pipeline_version="1.0.0",
        source_system="unit-test",
    )

    output_path = (
        tmp_path
        / "run-pipeline-meta"
        / "assay"
        / "datasets"
        / "assay_export.csv"
    )

    artifacts = pipeline.export(df, output_path)

    assert artifacts.metadata is not None
    assert artifacts.metadata_model is not None
    assert pipeline.export_metadata is artifacts.metadata_model

    with artifacts.metadata.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)

    checksums = payload["file_checksums"]
    assert artifacts.dataset.name in checksums
    assert artifacts.quality_report.name in checksums
    assert pipeline.export_metadata.checksums == checksums
    assert pipeline.export_metadata.checksums


def test_pipeline_export_fails_when_column_order_differs(tmp_path):
    """Export should fail-fast when dataframe columns drift from schema order."""

    class _OrderSchema(DataFrameModel):
        first: Series[int]
        second: Series[int]

        @classmethod
        def get_column_order(cls) -> list[str]:
            return ["first", "second"]

        _column_order = ["first", "second"]

    entity = "unit.order-check"
    version = "1.0.0"
    schema_registry.register(entity, version, _OrderSchema)

    class _OrderPipeline(PipelineBase):
        def extract(self, *args, **kwargs):  # noqa: D401, ANN001 - abstract contract stub
            raise NotImplementedError

        def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401
            return df

        def validate(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401
            return df

        def close_resources(self) -> None:  # noqa: D401
            return None

    try:
        config = load_config("configs/pipelines/assay.yaml")
        pipeline = _OrderPipeline(config, "run-order-mismatch")
        pipeline.primary_schema = _OrderSchema

        df = pd.DataFrame({"first": [1], "second": [2]})[["second", "first"]]

        with pytest.raises(ValueError, match="columns do not match"):
            pipeline.export(df, tmp_path / "order.csv")
    finally:
        registry_entry = schema_registry._registry.get(entity)
        if registry_entry is not None:
            registry_entry.pop(version, None)
            if not registry_entry:
                schema_registry._registry.pop(entity, None)
        schema_registry._metadata.pop((entity, version), None)


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

    assert metadata.get("sources") == []
    assert metadata["stage_durations_ms"]["load"] >= 0
    assert metadata["sort_keys"] == {}
    assert metadata["pii_secrets_policy"] == {}
    checksums = metadata["file_checksums"]
    assert "qc_missing_mappings.csv" not in checksums
    assert "qc_enrichment_metrics.csv" not in checksums
    assert "qc_summary.json" in checksums

    assert metadata["pii_secrets_policy"]["pii_expected"] is False
    assert metadata["sort_keys"] == []

    qc_artifacts = metadata["artifacts"].get("qc", {})
    assert "qc_missing_mappings" not in qc_artifacts
    assert "qc_enrichment_metrics" not in qc_artifacts
    assert qc_artifacts.get("qc_summary") == str(artifacts.qc_summary)
    assert "correlation_report" not in qc_artifacts
    assert "summary_statistics" not in qc_artifacts
    assert "dataset_metrics" not in qc_artifacts


def test_meta_yaml_matches_schema_registry(tmp_path, monkeypatch):
    """meta.yaml should mirror schema registry metadata when available."""

    _freeze_datetime(monkeypatch)

    class StubSchema(BaseSchema):
        value: Series[str]

        _column_order = [
            "index",
            "hash_row",
            "hash_business_key",
            "pipeline_version",
            "run_id",
            "source_system",
            "chembl_release",
            "extracted_at",
            "value",
        ]

    schema_registry.register(
        "stub",
        "1.2.3",
        StubSchema,
        schema_id="stub.output",
        na_policy="forbid",
        precision_policy="%.4f",
    )

    try:
        df = pd.DataFrame({"value": ["a", "b"]})
        writer = UnifiedOutputWriter("run-schema")
        metadata = OutputMetadata.from_dataframe(
            df,
            pipeline_version="9.9.9",
            source_system="unit-test",
            schema=StubSchema,
            run_id="run-schema",
        )

        artifacts = writer.write(
            df,
            tmp_path / "run-schema" / "stub" / "datasets" / "stub.csv",
            metadata=metadata,
        )

        assert artifacts.metadata is not None

        import yaml

        with artifacts.metadata.open(encoding="utf-8") as handle:
            contents = yaml.safe_load(handle)

        assert contents["schema_id"] == "stub.output"
        assert contents["schema_version"] == "1.2.3"
        assert contents["column_order_source"] == "schema_registry"
        assert contents["na_policy"] == "forbid"
        assert contents["precision_policy"] == "%.4f"
        assert "load" in contents["stage_durations_ms"]
        assert contents["sort_keys"] == {}
        assert contents["pii_secrets_policy"] == {}
    finally:
        stub_versions = schema_registry._registry.get("stub")
        if stub_versions is not None:
            stub_versions.pop("1.2.3", None)
            if not stub_versions:
                schema_registry._registry.pop("stub", None)
        schema_registry._metadata.pop(("stub", "1.2.3"), None)


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

    def boom(self, *args, **kwargs):  # noqa: ANN001 - matches DataFrame.to_json signature
        raise RuntimeError("boom")

    monkeypatch.setattr(pd.DataFrame, "to_json", boom, raising=False)

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


def test_write_dataframe_json_avoids_double_serialization(tmp_path, monkeypatch):
    """The dataframe serializer should not rely on ``json.loads``."""

    _freeze_datetime(monkeypatch)

    df = pd.DataFrame({"value": [1], "label": ["b"]})
    writer = UnifiedOutputWriter("run-json")

    def boom(*args, **kwargs):  # noqa: ANN001 - signature mirrors json.loads
        raise AssertionError("json.loads should not be invoked during JSON export")

    monkeypatch.setattr("bioetl.core.output_writer.json.loads", boom)

    target = tmp_path / "run-json" / "reports" / "dataset.json"

    writer.write_dataframe_json(df, target)

    assert target.exists()

