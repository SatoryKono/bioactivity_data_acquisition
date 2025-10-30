"""Tests for pipeline implementations."""

import json
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest
import requests
import yaml
from pandas.api.types import is_float_dtype, is_integer_dtype
from pandera.errors import SchemaErrors

from bioetl.config.loader import load_config
from bioetl.core.api_client import APIConfig, CircuitBreakerOpenError, UnifiedAPIClient
from bioetl.core.hashing import generate_hash_business_key
from bioetl.core.output_writer import OutputArtifacts
from bioetl.pipelines import (
    ActivityPipeline,
    AssayPipeline,
    DocumentPipeline,
    TargetPipeline,
    TestItemPipeline,
)
from bioetl.pipelines.assay import _NULLABLE_INT_COLUMNS
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import ActivitySchema, AssaySchema, TargetSchema, TestItemSchema
from bioetl.schemas.activity import COLUMN_ORDER as ACTIVITY_COLUMN_ORDER


@pytest.fixture
def assay_config():
    """Load assay pipeline config."""
    return load_config("configs/pipelines/assay.yaml")


@pytest.fixture
def activity_config():
    """Load activity pipeline config."""
    return load_config("configs/pipelines/activity.yaml")


@pytest.fixture
def testitem_config():
    """Load testitem pipeline config."""
    return load_config("configs/pipelines/testitem.yaml")


@pytest.fixture
def target_config():
    """Load target pipeline config."""
    return load_config("configs/pipelines/target.yaml")


@pytest.fixture
def document_config():
    """Load document pipeline config."""
    return load_config("configs/pipelines/document.yaml")


def test_pipeline_run_resets_per_run_state(assay_config, tmp_path):
    """Calling ``run`` twice should not accumulate QC records from previous runs."""

    class RecordingPipeline(PipelineBase):
        def extract(self, *args: Any, **kwargs: Any) -> pd.DataFrame:  # noqa: D401 - test stub
            return pd.DataFrame({"value": [1]})

        def transform(self, df: pd.DataFrame) -> pd.DataFrame:
            issue_number = len(self.validation_issues) + 1
            self.validation_issues.append({"metric": "validation_issue", "call": issue_number})

            self.qc_metrics["transform_calls"] = self.qc_metrics.get("transform_calls", 0) + 1

            summary_records = list(self.qc_summary_data.get("runs", []))
            summary_records.append({"call": issue_number})
            self.qc_summary_data["runs"] = summary_records

            new_missing = pd.DataFrame({"call": [issue_number]})
            self.qc_missing_mappings = pd.concat(
                [self.qc_missing_mappings, new_missing], ignore_index=True
            )

            new_enrichment = pd.DataFrame({"call": [issue_number]})
            self.qc_enrichment_metrics = pd.concat(
                [self.qc_enrichment_metrics, new_enrichment], ignore_index=True
            )

            self.stage_context["transform_call"] = self.stage_context.get("transform_call", 0) + 1
            return df

        def validate(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - test stub
            return df

        def export(
            self, df: pd.DataFrame, output_path: Path, *, extended: bool = False
        ) -> OutputArtifacts:  # noqa: D401 - test stub
            return OutputArtifacts(
                dataset=output_path,
                quality_report=output_path.with_suffix(".qc.csv"),
                run_directory=output_path.parent,
            )

    pipeline = RecordingPipeline(assay_config, "run-state-test")
    output_path = tmp_path / "dataset.csv"

    pipeline.run(output_path)

    assert len(pipeline.validation_issues) == 1
    assert pipeline.qc_metrics == {"transform_calls": 1}
    assert pipeline.qc_summary_data["runs"] == [{"call": 1}]
    assert list(pipeline.qc_missing_mappings["call"]) == [1]
    assert list(pipeline.qc_enrichment_metrics["call"]) == [1]
    assert pipeline.stage_context["transform_call"] == 1

    pipeline.run(output_path)

    assert len(pipeline.validation_issues) == 1
    assert pipeline.qc_metrics == {"transform_calls": 1}
    assert pipeline.qc_summary_data["runs"] == [{"call": 1}]
    assert list(pipeline.qc_missing_mappings["call"]) == [1]
    assert list(pipeline.qc_enrichment_metrics["call"]) == [1]
    assert pipeline.stage_context["transform_call"] == 1


def test_pipeline_run_does_not_reuse_runtime_limit(assay_config, tmp_path):
    """Runtime limits applied to one run should not constrain subsequent runs."""

    class LimitRecordingPipeline(PipelineBase):
        def __init__(self, config, run_id):  # noqa: D401, ANN001 - test stub signature
            super().__init__(config, run_id)
            self.observed_limits: list[int | None] = []

        def extract(self, *args: Any, **kwargs: Any) -> pd.DataFrame:  # noqa: D401 - test stub
            limit = self.get_runtime_limit()
            self.observed_limits.append(limit)
            rows = int(limit) if isinstance(limit, int) else 4
            return pd.DataFrame({"value": list(range(rows))})

        def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - test stub
            return df

        def validate(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - test stub
            return df

        def export(
            self, df: pd.DataFrame, output_path: Path, *, extended: bool = False
        ) -> OutputArtifacts:  # noqa: D401 - test stub
            self.output_writer.write_dataframe_csv(df, output_path)
            return OutputArtifacts(
                dataset=output_path,
                quality_report=output_path.with_suffix(".qc.csv"),
                run_directory=output_path.parent,
            )

        def close_resources(self) -> None:  # pragma: no cover - test stub
            return None

    pipeline = LimitRecordingPipeline(assay_config, "runtime-limit-reset")
    output_path = tmp_path / "dataset.csv"

    pipeline.runtime_options["limit"] = 2
    pipeline.runtime_options["sample"] = 2

    pipeline.run(output_path)

    assert pipeline.observed_limits == [2]
    assert "limit" not in pipeline.runtime_options
    assert "sample" not in pipeline.runtime_options

    pipeline.run(output_path)

    assert pipeline.observed_limits == [2, None]
    assert "limit" not in pipeline.runtime_options
    assert "sample" not in pipeline.runtime_options


def test_pipeline_run_closes_api_client(monkeypatch, assay_config, tmp_path):
    """``run`` should close UnifiedAPIClient instances when execution completes."""

    close_calls: list[UnifiedAPIClient] = []
    original_close = UnifiedAPIClient.close

    def _recording_close(self: UnifiedAPIClient) -> None:
        close_calls.append(self)
        original_close(self)

    monkeypatch.setattr(UnifiedAPIClient, "close", _recording_close)

    class ClosingPipeline(PipelineBase):
        def __init__(self, config, run_id):
            super().__init__(config, run_id)
            self.api_client = UnifiedAPIClient(
                APIConfig(name="test", base_url="https://example.org")
            )
            self.register_client(self.api_client)

        def extract(self, *args: Any, **kwargs: Any) -> pd.DataFrame:  # noqa: D401 - test stub
            return pd.DataFrame({"value": [1]})

        def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - test stub
            return df

        def validate(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - test stub
            return df

        def export(
            self, df: pd.DataFrame, output_path: Path, *, extended: bool = False
        ) -> OutputArtifacts:  # noqa: D401 - test stub
            self.output_writer.write_dataframe_csv(df, output_path)
            return OutputArtifacts(
                dataset=output_path,
                quality_report=output_path.with_suffix(".qc.csv"),
                run_directory=output_path.parent,
            )

    pipeline = ClosingPipeline(assay_config, "close-test")
    output_path = tmp_path / "dataset.csv"

    pipeline.run(output_path)

    assert close_calls, "UnifiedAPIClient.close should be invoked during pipeline teardown"


def test_pipeline_run_invokes_close(monkeypatch, assay_config, tmp_path):
    """``run`` should invoke ``close`` during teardown regardless of success."""

    class ClosingPipeline(PipelineBase):
        def extract(self, *args: Any, **kwargs: Any) -> pd.DataFrame:  # noqa: D401 - test stub
            return pd.DataFrame({"value": [1]})

        def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - test stub
            return df

        def validate(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - test stub
            return df

        def export(
            self, df: pd.DataFrame, output_path: Path, *, extended: bool = False
        ) -> OutputArtifacts:  # noqa: D401 - test stub
            self.output_writer.write_dataframe_csv(df, output_path)
            return OutputArtifacts(
                dataset=output_path,
                quality_report=output_path.with_suffix(".qc.csv"),
                run_directory=output_path.parent,
            )

    close_calls: list[PipelineBase] = []
    original_close = ClosingPipeline.close

    def _wrapped_close(self: PipelineBase) -> None:
        close_calls.append(self)
        original_close(self)

    monkeypatch.setattr(ClosingPipeline, "close", _wrapped_close)

    pipeline = ClosingPipeline(assay_config, "close-hook-test")
    output_path = tmp_path / "dataset.csv"

    pipeline.run(output_path)

    assert close_calls and close_calls[0] is pipeline


def test_pipeline_base_validate_uses_primary_schema(monkeypatch, assay_config):
    """Default ``validate`` should defer to the registered primary schema."""

    class SchemaPipeline(PipelineBase):
        def extract(self, *args: Any, **kwargs: Any) -> pd.DataFrame:  # pragma: no cover - unused
            raise NotImplementedError

        def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover - unused
            return df

        def validate(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover - unused
            schema = getattr(self, "primary_schema", None)
            if schema is None:
                return df

            dataset_name = getattr(self.config.pipeline, "name", None) or type(self).__name__
            dataset_label = str(dataset_name)

            return self._validate_with_schema(
                df,
                schema,
                dataset_name=dataset_label,
                severity="error",
                metric_name=f"schema.{dataset_label}",
            )

        def close_resources(self) -> None:  # pragma: no cover - unused
            return None

    pipeline = SchemaPipeline(assay_config, "base-validate-schema")

    recorded: dict[str, Any] = {}

    def _recording_validate(
        self,
        df: pd.DataFrame,
        schema: Any,
        *,
        dataset_name: str,
        severity: str = "error",
        metric_name: str | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        recorded.update(
            {
                "df": df,
                "schema": schema,
                "dataset_name": dataset_name,
                "severity": severity,
                "metric_name": metric_name,
            }
        )
        return df.assign(validated=True)

    monkeypatch.setattr(
        PipelineBase,
        "_validate_with_schema",
        _recording_validate,
        raising=False,
    )

    sentinel_schema = object()
    pipeline.primary_schema = sentinel_schema

    input_df = pd.DataFrame({"value": [1]})
    result_df = pipeline.validate(input_df)

    assert recorded["schema"] is sentinel_schema
    assert recorded["dataset_name"] == pipeline.config.pipeline.name
    assert recorded["severity"] == "error"
    assert recorded["metric_name"] == f"schema.{pipeline.config.pipeline.name}"
    assert "validated" in result_df.columns and result_df.loc[0, "validated"] is True


def test_pipeline_base_validate_without_primary_schema(monkeypatch, assay_config):
    """Default ``validate`` should no-op when a schema is not provided."""

    class DefaultPipeline(PipelineBase):
        def extract(self, *args: Any, **kwargs: Any) -> pd.DataFrame:  # pragma: no cover - unused
            raise NotImplementedError

        def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover - unused
            return df

        def validate(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover - unused
            schema = getattr(self, "primary_schema", None)
            if schema is None:
                return df

            dataset_name = getattr(self.config.pipeline, "name", None) or type(self).__name__
            dataset_label = str(dataset_name)

            return self._validate_with_schema(
                df,
                schema,
                dataset_name=dataset_label,
                severity="error",
                metric_name=f"schema.{dataset_label}",
            )

        def close_resources(self) -> None:  # pragma: no cover - unused
            return None

    pipeline = DefaultPipeline(assay_config, "base-validate-none")

    def _failing_validate(*args: Any, **kwargs: Any) -> pd.DataFrame:  # pragma: no cover - guard
        raise AssertionError("_validate_with_schema should not be called")

    monkeypatch.setattr(
        PipelineBase,
        "_validate_with_schema",
        _failing_validate,
        raising=False,
    )

    input_df = pd.DataFrame({"value": [1]})
    result_df = pipeline.validate(input_df)

    assert result_df is input_df


def test_export_prioritises_configured_column_order(tmp_path, assay_config):
    """Configured ``column_order`` should drive dataset column order on export."""

    class SimplePipeline(PipelineBase):
        def extract(self, *args: Any, **kwargs: Any) -> pd.DataFrame:  # noqa: D401 - test stub
            raise NotImplementedError

        def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - test stub
            return df

        def validate(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - test stub
            return df

    config = assay_config.model_copy(deep=True)
    config.determinism.column_order = ["alpha", "value"]

    pipeline = SimplePipeline(config, "run-config-order")

    source_df = pd.DataFrame({"value": [1, 2], "beta": ["x", "y"]})
    pipeline.set_export_metadata_from_dataframe(
        source_df,
        pipeline_version="1.0.0",
        source_system="test-system",
    )

    output_path = tmp_path / "dataset.csv"
    artifacts = pipeline.export(source_df, output_path)

    exported_df = pd.read_csv(artifacts.dataset)
    assert (
        exported_df.columns.tolist()[: len(config.determinism.column_order)]
        == config.determinism.column_order
    )
    assert exported_df["alpha"].isna().all()

    with artifacts.metadata.open("r", encoding="utf-8") as handle:
        metadata = yaml.safe_load(handle)

    assert metadata["column_order"] == exported_df.columns.tolist()
    assert metadata["column_count"] == len(exported_df.columns)
    expected_sources = sorted(
        name
        for name, source in pipeline.config.sources.items()
        if getattr(source, "enabled", True)
    )
    assert metadata["config_hash"] == pipeline.config.config_hash
    assert metadata.get("git_commit") is None
    assert metadata["sources"] == expected_sources


def test_export_uses_deterministic_float_format(tmp_path, assay_config):
    """Pipeline exports should respect determinism float precision and column order."""

    class DeterministicPipeline(PipelineBase):
        def extract(self, *args: Any, **kwargs: Any) -> pd.DataFrame:  # noqa: D401 - test stub
            raise NotImplementedError

        def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - test stub
            return df

        def validate(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - test stub
            return df

    config = assay_config.model_copy(deep=True)
    config.determinism.column_order = ["alpha", "value"]
    config.determinism.float_precision = 4

    pipeline = DeterministicPipeline(config, "run-format")

    source_df = pd.DataFrame({"beta": [0.111111], "value": [1.234567], "alpha": ["x"]})
    pipeline.set_export_metadata_from_dataframe(
        source_df,
        pipeline_version="1.0.0",
        source_system="test-system",
    )

    output_path = tmp_path / "deterministic.csv"
    artifacts = pipeline.export(source_df, output_path)

    content = artifacts.dataset.read_text(encoding="utf-8").splitlines()
    header = content[0].split(",")
    assert header[: len(config.determinism.column_order)] == config.determinism.column_order

    row = content[1].split(",")
    assert row[0] == "x"
    assert row[1] == "1.2346"
    assert row[2] == "0.1111"

    with artifacts.metadata.open("r", encoding="utf-8") as handle:
        metadata = yaml.safe_load(handle)

    assert metadata["column_order"] == header
    assert metadata["column_count"] == len(header)
    expected_sources = sorted(
        name
        for name, source in pipeline.config.sources.items()
        if getattr(source, "enabled", True)
    )
    assert metadata["config_hash"] == pipeline.config.config_hash
    assert metadata.get("git_commit") is None
    assert metadata["sources"] == expected_sources


def test_export_applies_configured_sorting(tmp_path, assay_config):
    """Export should apply configured sort order using stable sorting semantics."""

    class SortingPipeline(PipelineBase):
        def extract(self, *args: Any, **kwargs: Any) -> pd.DataFrame:  # noqa: D401 - test stub
            raise NotImplementedError

        def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - test stub
            return df

        def validate(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - test stub
            return df

    config = assay_config.model_copy(deep=True)
    config.determinism.sort.by = ["value", "missing"]
    config.determinism.sort.ascending = [False, True]

    pipeline = SortingPipeline(config, "run-sort")

    source_df = pd.DataFrame(
        {
            "value": [2, 2, 1, 2],
            "label": ["first", "second", "third", "fourth"],
        }
    )
    pipeline.set_export_metadata_from_dataframe(
        source_df,
        pipeline_version="1.0.0",
        source_system="test-system",
    )

    output_path = tmp_path / "sorted.csv"
    artifacts = pipeline.export(source_df, output_path)

    exported_df = pd.read_csv(artifacts.dataset)
    assert exported_df["value"].tolist() == [2, 2, 2, 1]
    assert exported_df["label"].tolist()[:3] == ["first", "second", "fourth"]


class TestAssayPipeline:
    """Tests for AssayPipeline."""

    @pytest.fixture(autouse=True)
    def _mock_status(self, monkeypatch):
        """Return a deterministic status payload to prevent live network calls."""

        def _status_stub(self, url, params=None, method="GET", **kwargs):
            if url == "/status.json":
                return {"chembl_db_version": "ChEMBL_TEST"}
            raise AssertionError(f"Unexpected request during test: {url}")

        monkeypatch.setattr(UnifiedAPIClient, "request_json", _status_stub)

    def test_cache_key_composition(self, assay_config, monkeypatch):
        """Ensure cache keys include the captured release."""

        call_count = {"status": 0}

        def _request(self, url, params=None, method="GET", **kwargs):
            if url == "/status.json":
                call_count["status"] += 1
                return {"chembl_db_version": "ChEMBL_36"}
            raise AssertionError(f"Unexpected URL {url}")

        monkeypatch.setattr(UnifiedAPIClient, "request_json", _request)

        pipeline = AssayPipeline(assay_config, "run-cache")
        assert call_count["status"] == 1
        assert pipeline.chembl_release == "ChEMBL_36"
        assert pipeline._make_cache_key("CHEMBL1") == "assay:ChEMBL_36:CHEMBL1"

    def test_init(self, assay_config):
        """Test pipeline initialization."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = AssayPipeline(assay_config, run_id)
        assert pipeline.config == assay_config
        assert pipeline.run_id == run_id

    def test_extract_empty_file(self, assay_config, tmp_path):
        """Test extraction with empty file."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = AssayPipeline(assay_config, run_id)

        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("assay_chembl_id,description\n")

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_extract_with_data(self, assay_config, tmp_path):
        """Test extraction with sample data."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = AssayPipeline(assay_config, run_id)

        csv_path = tmp_path / "assay.csv"
        csv_path.write_text(
            "assay_chembl_id,Target TYPE,description,target_chembl_id\n"
            "CHEMBL1,Enzyme,Test assay,CHEMBL101\n"
        )

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0  # May be limited by nrows=10

    def test_transform_adds_metadata(self, assay_config, monkeypatch):
        """Test transformation adds pipeline metadata."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = AssayPipeline(assay_config, run_id)

        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "description": ["  Test Assay  "],
            }
        )

        mock_assay_data = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_parameters_json": [None],
                "variant_sequence_json": [None],
                "assay_classifications": [None],
            }
        )

        monkeypatch.setattr(pipeline, "_fetch_assay_data", lambda ids: mock_assay_data)
        monkeypatch.setattr(pipeline, "_fetch_target_reference_data", lambda ids: pd.DataFrame())
        monkeypatch.setattr(
            pipeline, "_fetch_assay_class_reference_data", lambda ids: pd.DataFrame()
        )

        result = pipeline.transform(df)
        assert "pipeline_version" in result.columns
        assert "source_system" in result.columns
        assert "extracted_at" in result.columns

    def test_fetch_assay_data_uses_cache(self, assay_config, monkeypatch):
        """Assay payloads should be cached by release-qualified key."""

        counts = {"status": 0, "fetch": 0}

        def _request(self, url, params=None, method="GET", **kwargs):
            if url == "/status.json":
                counts["status"] += 1
                return {"chembl_db_version": "ChEMBL_37"}
            if url == "/assay.json":
                counts["fetch"] += 1
                return {"assays": [{"assay_chembl_id": "CHEMBL1", "assay_type": "BIND"}]}
            raise AssertionError(f"Unexpected URL {url}")

        monkeypatch.setattr(UnifiedAPIClient, "request_json", _request)

        pipeline = AssayPipeline(assay_config, "run-cache-hit")
        first = pipeline._fetch_assay_data(["CHEMBL1"])
        second = pipeline._fetch_assay_data(["CHEMBL1"])

        assert counts["fetch"] == 1
        assert not first.empty
        assert not second.empty
        assert second.iloc[0]["assay_chembl_id"] == "CHEMBL1"

        metrics = pipeline.run_metadata.get("assay_fetch_metrics", {})
        assert metrics.get("cache_hits") == 1
        assert metrics.get("success_count") == 2
        assert metrics.get("fallback_total") == 0

    def test_fetch_assay_data_fallback_metadata(self, assay_config, monkeypatch):
        """Fallback records should include error metadata when requests fail."""

        def _request(self, url, params=None, method="GET", **kwargs):
            if url == "/status.json":
                return {"chembl_db_version": "ChEMBL_F"}
            raise CircuitBreakerOpenError("breaker open for tests")

        monkeypatch.setattr(UnifiedAPIClient, "request_json", _request)

        pipeline = AssayPipeline(assay_config, "run-fallback")
        df = pipeline._fetch_assay_data(["CHEMBL_FAIL"])

        assert not df.empty
        row = df.iloc[0]
        assert row["assay_chembl_id"] == "CHEMBL_FAIL"
        assert row["source_system"] == "ChEMBL_FALLBACK"
        assert row["chembl_release"] == "ChEMBL_F"
        assert row["git_commit"] == pipeline.git_commit
        assert row["config_hash"] == pipeline.config_hash
        assert (
            row["fallback_error_message"].startswith("Circuit")
            or "breaker" in row["fallback_error_message"]
        )
        assert row["run_id"] == "run-fallback"

        metrics = pipeline.run_metadata.get("assay_fetch_metrics", {})
        assert metrics.get("fallback_total") == 1
        assert metrics.get("fallback_by_reason", {}).get("circuit_open") == 1
        assert pipeline.qc_metrics.get("assay_fallback_total") == 1

    def test_validate_schema_errors_capture(self, assay_config):
        """Schema violations should be surfaced as validation issues."""

        run_id = str(uuid.uuid4())[:8]
        pipeline = AssayPipeline(assay_config, run_id)
        df = pd.DataFrame([_build_assay_row("CHEMBL0", 0, None)]).convert_dtypes()
        df = df.drop(columns=["assay_chembl_id"])  # force schema failure

        with pytest.raises(ValueError):
            pipeline.validate(df)

        assert pipeline.validation_issues
        issue = pipeline.validation_issues[0]
        assert issue["issue_type"] == "schema"
        assert issue["severity"] == "error"

    def test_validate_accepts_nullable_integer_na(self, assay_config):
        """Nullable integer columns should accept Pandas <NA> values."""

        run_id = str(uuid.uuid4())[:8]
        pipeline = AssayPipeline(assay_config, run_id)

        row = _build_assay_row("CHEMBLNA", 0, None)
        for column in _NULLABLE_INT_COLUMNS:
            row[column] = pd.NA

        df = pd.DataFrame([row]).convert_dtypes()

        validated = pipeline.validate(df)

        for column in _NULLABLE_INT_COLUMNS:
            assert str(validated[column].dtype) == "Int64"
            assert validated[column].isna().all()

    def test_validate_coerces_fractional_nullable_ints(self, assay_config):
        """Fractional values in nullable integer columns should coerce to <NA>."""

        pipeline = AssayPipeline(assay_config, "run-fractional")

        fractional_row = _build_assay_row("CHEMBLFRACT1", 0, None)
        fractional_row.update(
            {
                "assay_class_id": "42.5",
                "component_count": "7.25",
                "variant_id": "13.7",
            }
        )

        integral_row = _build_assay_row("CHEMBLFRACT2", 1, None)
        integral_row.update(
            {
                "assay_class_id": "77",
                "component_count": 3.0,
                "variant_id": "901",
            }
        )

        noisy_row = _build_assay_row("CHEMBLFRACT3", 2, None)
        noisy_row.update(
            {
                "assay_class_id": "not-a-number",
                "component_count": "",
                "variant_id": None,
            }
        )

        df = pd.DataFrame([fractional_row, integral_row, noisy_row]).convert_dtypes()

        validated = pipeline.validate(df)

        # Fractional values should become <NA>, valid integers should round-trip.
        assert pd.isna(validated.loc[0, "assay_class_id"])
        assert validated.loc[1, "assay_class_id"] == 77
        assert pd.isna(validated.loc[2, "assay_class_id"])

        assert pd.isna(validated.loc[0, "component_count"])
        assert validated.loc[1, "component_count"] == 3
        assert pd.isna(validated.loc[2, "component_count"])

        assert pd.isna(validated.loc[0, "variant_id"])
        assert validated.loc[1, "variant_id"] == 901
        assert pd.isna(validated.loc[2, "variant_id"])

        for column in ("assay_class_id", "component_count", "variant_id"):
            assert str(validated[column].dtype) == "Int64"

    def test_validation_issues_reflected_in_quality_report(self, assay_config, tmp_path):
        """Referential integrity warnings should appear in QC artifacts."""

        paths = assay_config.paths.model_copy(
            update={
                "input_root": tmp_path,
                "output_root": tmp_path / "out",
            }
        )
        qc_settings = assay_config.qc.model_copy(
            update={"thresholds": {"assay.target_missing_ratio": 0.75}}
        )
        config = assay_config.model_copy(update={"paths": paths, "qc": qc_settings}, deep=True)

        pd.DataFrame({"target_chembl_id": ["CHEMBL1000"]}).to_csv(
            tmp_path / "target.csv", index=False
        )

        run_id = str(uuid.uuid4())[:8]
        pipeline = AssayPipeline(config, run_id)

        df = pd.DataFrame(
            [
                _build_assay_row("CHEMBL1", 0, "CHEMBL1000"),
                _build_assay_row("CHEMBL2", 1, "CHEMBL9999"),
            ]
        ).convert_dtypes()

        validated = pipeline.validate(df)

        assert len(pipeline.validation_issues) == 1
        issue = pipeline.validation_issues[0]
        assert issue["issue_type"] == "referential_integrity"
        assert issue["count"] == 1

        artifacts = pipeline.export(validated, tmp_path / "out" / "assay.csv")
        quality_df = pd.read_csv(artifacts.quality_report)
        validation_rows = quality_df[quality_df["metric"] == "validation_issue"]

        assert not validation_rows.empty
        assert validation_rows["issue_type"].fillna("").str.contains("referential_integrity").any()

    def test_quality_report_without_validation_issues(self, assay_config, tmp_path):
        """A clean dataset should not emit validation_issue rows in QC."""

        paths = assay_config.paths.model_copy(
            update={
                "input_root": tmp_path,
                "output_root": tmp_path / "out",
            }
        )
        qc_settings = assay_config.qc.model_copy(
            update={"thresholds": {"assay.target_missing_ratio": 0.75}}
        )
        config = assay_config.model_copy(update={"paths": paths, "qc": qc_settings}, deep=True)

        pd.DataFrame({"target_chembl_id": ["CHEMBL1000", "CHEMBL2000"]}).to_csv(
            tmp_path / "target.csv", index=False
        )

        run_id = str(uuid.uuid4())[:8]
        pipeline = AssayPipeline(config, run_id)

        df = pd.DataFrame(
            [
                _build_assay_row("CHEMBL1", 0, "CHEMBL1000"),
                _build_assay_row("CHEMBL2", 1, "CHEMBL2000"),
            ]
        ).convert_dtypes()

        validated = pipeline.validate(df)
        assert not pipeline.validation_issues

        artifacts = pipeline.export(validated, tmp_path / "out" / "assay.csv")
        quality_df = pd.read_csv(artifacts.quality_report)

        assert "metric" in quality_df.columns
        assert not (quality_df["metric"] == "validation_issue").any()

    def test_fetch_assay_data_respects_max_url_length(self, assay_config, monkeypatch):
        """Batch requests must respect the configured max URL length."""

        config = assay_config.model_copy(deep=True)
        config.sources["chembl"].max_url_length = 80

        def _request(self, url, params=None, method="GET", **kwargs):
            if url == "/status.json":
                return {"chembl_db_version": "ChEMBL_TEST"}
            if url == "/assay.json":
                joined = params.get("assay_chembl_id__in", "") if params else ""
                batch_ids = [value for value in joined.split(",") if value]
                return {
                    "assays": [
                        {
                            "assay_chembl_id": assay_id,
                        }
                        for assay_id in batch_ids
                    ]
                }
            raise AssertionError(f"Unexpected URL {url}")

        monkeypatch.setattr(UnifiedAPIClient, "request_json", _request)

        pipeline = AssayPipeline(config, "run-max-url")

        assay_ids = [f"CHEMBL{1000 + idx}" for idx in range(6)]
        df = pipeline._fetch_assay_data(assay_ids)

        assert not df.empty
        assert sorted(df["assay_chembl_id"].tolist()) == sorted(assay_ids)

        # Ensure each request would conform to the URL limit
        batches = pipeline._split_assay_ids_by_url_length(assay_ids)
        assert len(batches) > 1
        for batch in batches:
            url = pipeline._build_assay_request_url(batch)
            assert len(url) <= pipeline.max_url_length or len(batch) == 1

    def test_transform_expands_and_enriches(self, assay_config, monkeypatch, caplog):
        """Ensure parameters, variants, and classifications survive transform with enrichment."""

        run_id = str(uuid.uuid4())[:8]
        pipeline = AssayPipeline(assay_config, run_id)

        input_df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
            }
        )

        assay_payload = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "target_chembl_id": ["CHEMBL2"],
                "assay_parameters_json": [
                    json.dumps(
                        [
                            {
                                "type": "CONC",
                                "relation": ">",
                                "value": 1.5,
                                "units": "nM",
                                "text_value": "1.5",
                                "standard_type": "IC50",
                                "standard_value": 1.5,
                                "standard_units": "nM",
                            },
                            {
                                "type": "TEMP",
                                "relation": "=",
                                "value": 37,
                                "units": "C",
                            },
                        ]
                    )
                ],
                "variant_sequence_json": [
                    json.dumps(
                        [
                            {
                                "variant_id": 101,
                                "base_accession": "P12345",
                                "mutation": "A50T",
                                "variant_seq": "MTEYKLVVVG",
                                "accession_reported": "Q99999",
                            }
                        ]
                    )
                ],
                "assay_classifications": [
                    json.dumps(
                        [
                            {
                                "assay_class_id": 501,
                                "bao_id": "BAO_0000001",
                                "class_type": "primary",
                                "l1": "Level1",
                                "l2": "Level2",
                                "l3": "Level3",
                                "description": "Example class",
                            },
                            {
                                "assay_class_id": 502,
                                "bao_id": "BAO_0000002",
                                "class_type": "secondary",
                                "l1": "L1",
                                "l2": "L2",
                                "l3": "L3",
                                "description": "Another class",
                            },
                        ]
                    )
                ],
            }
        )

        monkeypatch.setattr(pipeline, "_fetch_assay_data", lambda ids: assay_payload)

        target_reference = pd.DataFrame(
            {
                "target_chembl_id": ["CHEMBL2"],
                "pref_name": ["Target X"],
                "organism": ["Homo sapiens"],
                "target_type": ["SINGLE PROTEIN"],
                "species_group_flag": [1],
                "tax_id": [9606],
                "component_count": [1],
                "unexpected_column": ["drop"],
            }
        )

        assay_class_reference = pd.DataFrame(
            {
                "assay_class_id": [501, 502],
                "assay_class_bao_id": ["BAO_0000001", "BAO_0000002"],
                "assay_class_type": ["primary", "secondary"],
                "assay_class_l1": ["Level1", "L1"],
                "assay_class_l2": ["Level2", "L2"],
                "assay_class_l3": ["Level3", "L3"],
                "assay_class_description": ["Example class", "Another class"],
                "extra_field": ["drop", "drop"],
            }
        )

        monkeypatch.setattr(
            pipeline,
            "_fetch_target_reference_data",
            lambda ids: target_reference,
        )
        monkeypatch.setattr(
            pipeline,
            "_fetch_assay_class_reference_data",
            lambda ids: assay_class_reference,
        )

        result = pipeline.transform(input_df)

        # Canonical ordering maintained and row_subtype removed from export columns
        assert list(result.columns) == AssaySchema.Config.column_order
        assert "row_subtype" not in result.columns

        # Integer fields must use nullable Int64 dtype to satisfy schema coercion
        expected_integer_columns = [
            "assay_class_id",
            "component_count",
            "variant_id",
            "species_group_flag",
            "tax_id",
        ]
        for column in expected_integer_columns:
            if column in result.columns:
                assert is_integer_dtype(result[column]), f"{column} should be Int64"

        # Expect one base row, two parameter rows, and two classification rows (5 total)
        assert len(result) == 5

        base_rows = result[result["assay_param_type"].isna() & result["assay_class_id"].isna()]
        assert len(base_rows) == 1
        assert base_rows.iloc[0]["pref_name"] == "Target X"

        param_rows = result[result["assay_param_type"].notna()]
        assert len(param_rows) == 2
        assert {"CONC", "TEMP"} == set(param_rows["assay_param_type"].dropna())
        assert param_rows["assay_class_id"].isna().all()

        class_rows = result[result["assay_class_id"].notna()]
        assert len(class_rows) == 2
        assert class_rows["assay_param_type"].isna().all()
        # Enrichment should preserve whitelist fields and drop unexpected columns
        assert "extra_field" not in result.columns
        assert class_rows["assay_class_description"].notna().all()

        # Join loss logging triggered when enrichment loses records (simulate by clearing target data)
        join_loss_payload = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL9"],
                "target_chembl_id": ["CHEMBL_NOPE"],
                "assay_parameters_json": [None],
                "variant_sequence_json": [None],
                "assay_classifications": [None],
            }
        )
        monkeypatch.setattr(pipeline, "_fetch_assay_data", lambda ids: join_loss_payload)
        monkeypatch.setattr(pipeline, "_fetch_target_reference_data", lambda ids: pd.DataFrame())
        with caplog.at_level("WARNING"):
            pipeline.transform(
                pd.DataFrame({"assay_chembl_id": ["CHEMBL9"], "target_chembl_id": ["CHEMBL_NOPE"]})
            )
        assert any("target_enrichment_join_loss" in rec.message for rec in caplog.records)


class TestActivityPipeline:
    """Tests for ActivityPipeline."""

    @staticmethod
    def _build_activity_dataframe(activity_id: int = 1, index_value: int = 0) -> pd.DataFrame:
        """Create a minimal valid activity dataframe for validation tests."""
        base_hash = format(activity_id, "x").zfill(64)
        row = {
            "activity_id": activity_id,
            "molecule_chembl_id": "CHEMBL1",
            "assay_chembl_id": "CHEMBL2",
            "target_chembl_id": "CHEMBL3",
            "document_chembl_id": "CHEMBL4",
            "published_type": "IC50",
            "published_relation": "=",
            "published_value": 10.0,
            "published_units": "nM",
            "standard_type": "IC50",
            "standard_relation": "=",
            "standard_value": 9.5,
            "standard_units": "nM",
            "standard_flag": 1,
            "pchembl_value": 7.0,
            "lower_bound": 8.0,
            "upper_bound": 11.0,
            "is_censored": False,
            "activity_comment": None,
            "data_validity_comment": None,
            "bao_endpoint": "BAO_0000190",
            "bao_format": "BAO_0000357",
            "bao_label": "single protein format",
            "canonical_smiles": "C1=CC=CC=C1",
            "target_organism": "Homo Sapiens",
            "target_tax_id": 9606,
            "activity_properties": "[]",
            "compound_key": "CHEMBL1|IC50|CHEMBL3",
            "is_citation": True,
            "high_citation_rate": False,
            "exact_data_citation": False,
            "rounded_data_citation": False,
            "potential_duplicate": 0,
            "uo_units": "UO_0000065",
            "qudt_units": "http://qudt.org/vocab/unit/NanoMOL-PER-L",
            "src_id": 1,
            "action_type": "inhibition",
            "bei": 1.0,
            "sei": 1.0,
            "le": 1.0,
            "lle": 1.0,
            "pipeline_version": "1.0.0",
            "source_system": "chembl",
            "chembl_release": "36",
            "extracted_at": "2024-01-01T00:00:00+00:00",
            "hash_business_key": base_hash,
            "hash_row": format(activity_id + 1000, "x").zfill(64),
            "index": index_value,
        }
        df = pd.DataFrame([row])
        return df[[col for col in ACTIVITY_COLUMN_ORDER if col in df.columns]]

    def test_init(self, activity_config):
        """Test pipeline initialization."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = ActivityPipeline(activity_config, run_id)
        assert pipeline.config == activity_config
        assert pipeline.run_id == run_id

    def test_extract_empty_file(self, activity_config, tmp_path):
        """Test extraction with empty file."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = ActivityPipeline(activity_config, run_id)

        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("activity_id,molecule_chembl_id\n")

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_extract_applies_runtime_limit_before_fetch(
        self, activity_config, tmp_path, monkeypatch
    ) -> None:
        """Ensure runtime limits constrain the number of API lookups."""

        run_id = "limit-run"
        pipeline = ActivityPipeline(activity_config, run_id)
        pipeline.runtime_options["limit"] = 2

        captured_ids: list[int] = []

        def fake_extract(ids: list[int]) -> pd.DataFrame:
            captured_ids.extend(ids)
            return pd.DataFrame({"activity_id": ids})

        monkeypatch.setattr(pipeline, "_extract_from_chembl", fake_extract)

        input_df = pd.DataFrame({"activity_id": [1, 2, 3, 4]})
        input_path = tmp_path / "activities.csv"
        input_df.to_csv(input_path, index=False)

        result = pipeline.extract(input_file=input_path)

        assert captured_ids == [1, 2]
        assert len(result) == 2

    def test_extract_from_chembl_splits_batches_by_url_length(
        self, activity_config, monkeypatch
    ) -> None:
        """Ensure batch iteration respects the configured URL length limit."""

        activity_config.sources["chembl"].max_url_length = 60
        activity_config.cache.enabled = False

        pipeline = ActivityPipeline(activity_config, "url-limit")
        pipeline.api_client.config.base_url = "https://chembl.test"

        monkeypatch.setattr(pipeline, "_store_batch_in_cache", lambda *args, **kwargs: None)
        monkeypatch.setattr(pipeline, "_load_batch_from_cache", lambda batch_ids: None)

        recorded_calls: list[str] = []

        def fake_request(
            url: str, params: dict[str, Any] | None = None, **_: Any
        ) -> dict[str, Any]:
            assert params is not None
            recorded_calls.append(params.get("activity_id__in", ""))
            ids = [int(value) for value in params.get("activity_id__in", "").split(",") if value]
            return {"activities": [{"activity_id": activity_id} for activity_id in ids]}

        monkeypatch.setattr(pipeline.api_client, "request_json", fake_request)

        result = pipeline._extract_from_chembl([1, 2, 3, 4])

        assert recorded_calls == ["1,2", "3,4"]
        assert sorted(result["activity_id"].dropna().astype(int).tolist()) == [1, 2, 3, 4]

    def test_normalize_activity_field_mapping(self, activity_config):
        """Dedicated mappers should normalize every field as specified."""

        run_id = str(uuid.uuid4())[:8]
        pipeline = ActivityPipeline(activity_config, run_id)

        raw_activity = {
            "activity_id": "123 ",
            "molecule_chembl_id": " chembl42 ",
            "assay_chembl_id": "ChEmBl2 ",
            "target_chembl_id": "cheMBL3",
            "document_chembl_id": " chembl4 ",
            "type": " ic50 ",
            "relation": "≥",
            "value": " 10 ",
            "units": " nm ",
            "standard_type": " ki ",
            "standard_relation": "≤",
            "standard_value": " 9.5 ",
            "standard_units": "ug/ml",
            "standard_flag": "1",
            "standard_lower_value": "8",
            "standard_upper_value": "12",
            "pchembl_value": "7.3",
            "activity_comment": "  comment ",
            "data_validity_comment": "Exact data citation confirmed",
            "bao_endpoint": " bao_0000190 ",
            "bao_format": "BAO_0000357",
            "bao_label": " single   protein format ",
            "canonical_smiles": " CC(O) ",
            "target_organism": "homo   sapiens",
            "target_tax_id": "9606",
            "potential_duplicate": "1",
            "uo_units": "uo_0000065",
            "qudt_units": " http://qudt.org/unit ",
            "src_id": "42",
            "action_type": " inhibition ",
            "activity_properties": [
                {"name": "citation_count", "value": "120"},
                {"name": "note", "value": " curated "},
            ],
            "ligand_efficiency": {"bei": "10", "sei": "2", "le": "0.5", "lle": "4"},
        }

        normalized = pipeline._normalize_activity(raw_activity)

        assert normalized["activity_id"] == 123
        assert normalized["molecule_chembl_id"] == "CHEMBL42"
        assert normalized["assay_chembl_id"] == "CHEMBL2"
        assert normalized["target_chembl_id"] == "CHEMBL3"
        assert normalized["document_chembl_id"] == "CHEMBL4"
        assert normalized["published_type"] == "IC50"
        assert normalized["published_relation"] == ">="
        assert normalized["published_units"] == "nM"
        assert normalized["standard_type"] == "KI"
        assert normalized["standard_relation"] == "<="
        assert normalized["standard_units"] == "µg/mL"
        assert normalized["standard_flag"] == 1
        assert normalized["lower_bound"] == 8.0
        assert normalized["upper_bound"] == 12.0
        assert normalized["is_censored"] is True
        assert normalized["bao_endpoint"] == "BAO_0000190"
        assert normalized["canonical_smiles"] == "CC(O)"
        assert normalized["target_organism"] == "Homo Sapiens"
        assert normalized["target_tax_id"] == 9606
        assert normalized["potential_duplicate"] == 1
        assert normalized["uo_units"] == "UO_0000065"
        assert normalized["qudt_units"] == "http://qudt.org/unit"
        assert normalized["src_id"] == 42
        assert normalized["action_type"] == "inhibition"
        assert normalized["bei"] == 10.0
        assert normalized["sei"] == 2.0
        assert normalized["le"] == 0.5
        assert normalized["lle"] == 4.0
        assert normalized["compound_key"] == "CHEMBL42|KI|CHEMBL3"
        assert normalized["is_citation"] is True
        assert normalized["exact_data_citation"] is True
        assert normalized["rounded_data_citation"] is False
        assert normalized["high_citation_rate"] is True

        properties = json.loads(normalized["activity_properties"])
        assert properties[0]["name"] == "citation_count"
        assert properties[0]["value"] == 120.0
        assert properties[1]["value"] == "curated"

    def test_normalize_activity_clamps_negative_measurements(self, activity_config, caplog):
        """Negative measurement values should be sanitised to preserve schema rules."""

        run_id = str(uuid.uuid4())[:8]
        pipeline = ActivityPipeline(activity_config, run_id)

        raw_activity = {
            "activity_id": 999,
            "molecule_chembl_id": "CHEMBL999",
            "target_chembl_id": "CHEMBL42",
            "type": "IC50",
            "value": -8,
            "standard_type": "IC50",
            "standard_value": -12.5,
            "pchembl_value": -1.7,
        }

        caplog.set_level("WARNING")
        normalized = pipeline._normalize_activity(raw_activity)

        assert normalized["published_value"] is None
        assert normalized["standard_value"] is None
        assert normalized["pchembl_value"] == pytest.approx(-1.7)

        assert caplog.text.count("non_negative_float_sanitized") >= 2

    def test_validate_records_qc_metrics(self, activity_config):
        """Validation should record QC metrics for downstream reporting."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = ActivityPipeline(activity_config, run_id)

        df = self._build_activity_dataframe(activity_id=1)

        result = pipeline.validate(df)
        assert len(result) == 1

        report = pipeline.last_validation_report
        assert report is not None
        assert report["metrics"]["duplicates"]["value"] == 0
        assert report["metrics"]["null_rate"]["value"] == 0
        assert report["metrics"]["invalid_units"]["value"] == 0
        assert report["metrics"]["null_fraction.standard_value"]["value"] == 0
        assert report["metrics"]["null_fraction.standard_type"]["value"] == 0
        assert report["metrics"]["null_fraction.molecule_chembl_id"]["value"] == 0

        issue_metrics = {
            issue["metric"]: issue for issue in pipeline.validation_issues if issue.get("metric")
        }
        assert {
            "qc.duplicates",
            "qc.null_rate",
            "qc.null_fraction.standard_value",
            "qc.null_fraction.standard_type",
            "qc.null_fraction.molecule_chembl_id",
            "qc.invalid_units",
            "schema.validation",
        }.issubset(issue_metrics.keys())
        assert issue_metrics["schema.validation"]["status"] == "passed"
        assert issue_metrics["schema.validation"]["severity"] == "info"

    def test_validate_coerces_null_retry_after_to_float(self, activity_config):
        """Fallback retry-after column should normalise object NA values."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = ActivityPipeline(activity_config, run_id)

        df = self._build_activity_dataframe(activity_id=7)
        df["fallback_retry_after_sec"] = pd.Series([pd.NA], dtype="object")

        validated = pipeline.validate(df)

        assert is_float_dtype(validated["fallback_retry_after_sec"])
        assert pd.isna(validated.at[0, "fallback_retry_after_sec"])

    def test_validate_raises_on_duplicates(self, activity_config):
        """Duplicate activities should fail validation."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = ActivityPipeline(activity_config, run_id)

        df = pd.concat(
            [
                self._build_activity_dataframe(activity_id=1, index_value=0),
                self._build_activity_dataframe(activity_id=1, index_value=1),
            ],
            ignore_index=True,
        )

        with pytest.raises(ValueError):
            pipeline.validate(df)

        report = pipeline.last_validation_report
        assert report is not None
        assert report["metrics"]["duplicates"]["value"] == 1
        assert "duplicates" in report.get("failing_metrics", {})

    def test_validate_honors_zero_null_threshold(self, activity_config):
        """Null QC thresholds should respect explicit zero values."""

        activity_config.qc.thresholds["activity.null_fraction"] = 1.0
        activity_config.qc.thresholds["activity.null_fraction.standard_value"] = 0.0

        run_id = str(uuid.uuid4())[:8]
        pipeline = ActivityPipeline(activity_config, run_id)

        df = self._build_activity_dataframe(activity_id=1)
        df.loc[0, "standard_value"] = None

        with pytest.raises(ValueError):
            pipeline.validate(df)

        report = pipeline.last_validation_report
        assert report is not None
        column_metric = report["metrics"].get("null_fraction.standard_value")
        assert column_metric is not None
        assert column_metric["threshold"] == 0.0
        assert column_metric["value"] == 1.0
        assert report["failing_metrics"].get("null_fraction.standard_value") == column_metric

    def test_validate_invalid_relation_triggers_pandera_error(self, activity_config):
        """Invalid standard relation should raise a Pandera schema error."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = ActivityPipeline(activity_config, run_id)

        df = self._build_activity_dataframe(activity_id=1)
        df.loc[0, "standard_relation"] = "!="

        with pytest.raises(SchemaErrors):
            pipeline.validate(df)

        report = pipeline.last_validation_report
        assert report is not None
        schema_report = report.get("schema_validation")
        assert schema_report is not None
        assert schema_report.get("status") == "failed"
        schema_issues = [
            issue
            for issue in pipeline.validation_issues
            if issue.get("metric") == "schema.validation"
        ]
        assert schema_issues
        assert all(issue.get("severity") == "critical" for issue in schema_issues)

    def test_validate_invalid_unit_triggers_pandera_error(self, activity_config):
        """Invalid standard unit should raise a Pandera schema error."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = ActivityPipeline(activity_config, run_id)

        df = self._build_activity_dataframe(activity_id=1)
        df.loc[0, "standard_units"] = "INVALID_UNIT"

        with pytest.raises(SchemaErrors):
            pipeline.validate(df)

        report = pipeline.last_validation_report
        assert report is not None
        schema_report = report.get("schema_validation")
        assert schema_report is not None
        assert schema_report.get("status") == "failed"
        schema_issues = [
            issue
            for issue in pipeline.validation_issues
            if issue.get("metric") == "schema.validation"
        ]
        assert schema_issues
        assert all(issue.get("severity") == "critical" for issue in schema_issues)

    def test_validate_records_fallback_diagnostics(self, activity_config):
        """Fallback rows should be captured in diagnostics and QC outputs."""

        # Настраиваем пороговые значения QC для fallback записей
        activity_config.qc.thresholds["activity.null_fraction.standard_value"] = 1.0
        activity_config.qc.thresholds["activity.null_fraction.standard_type"] = 1.0
        activity_config.qc.thresholds["activity.null_fraction.molecule_chembl_id"] = 1.0
        activity_config.qc.thresholds["null_rate_critical"] = 1.0
        activity_config.qc.thresholds["invalid_units"] = 1.0

        run_id = str(uuid.uuid4())[:8]
        pipeline = ActivityPipeline(activity_config, run_id)

        primary = self._build_activity_dataframe(activity_id=101)
        fallback = self._build_activity_dataframe(activity_id=202)
        fallback.loc[:, "source_system"] = "ChEMBL_FALLBACK"
        fallback.loc[:, "molecule_chembl_id"] = None
        fallback.loc[:, "assay_chembl_id"] = None
        fallback.loc[:, "target_chembl_id"] = None
        fallback.loc[:, "document_chembl_id"] = None
        fallback.loc[:, "standard_value"] = None
        fallback.loc[:, "fallback_reason"] = "not_in_response"
        fallback.loc[:, "fallback_error_type"] = "HTTPError"
        fallback.loc[:, "fallback_error_message"] = "Activity not found"
        fallback.loc[:, "fallback_http_status"] = 404
        fallback.loc[:, "fallback_error_code"] = "not_found"
        fallback.loc[:, "fallback_retry_after_sec"] = 1.5
        fallback.loc[:, "fallback_attempt"] = 3
        fallback.loc[:, "fallback_timestamp"] = "2024-01-01T00:00:00+00:00"

        combined = pd.concat([primary, fallback], ignore_index=True, sort=False)

        transformed = pipeline.transform(combined)
        validated = pipeline.validate(transformed)

        assert len(validated) == 2

        fallback_table = pipeline.additional_tables.get("activity_fallback_records")
        assert fallback_table is not None
        assert len(fallback_table.dataframe) == 1
        assert is_float_dtype(fallback_table.dataframe["fallback_retry_after_sec"])  # noqa: PD011
        fallback_row = fallback_table.dataframe.iloc[0]
        assert fallback_row["activity_id"] == 202
        assert fallback_row["fallback_reason"] == "not_in_response"
        assert fallback_row["fallback_http_status"] == 404
        assert fallback_row["fallback_error_message"] == "Activity not found"

        summary = pipeline.qc_summary_data.get("fallbacks")
        assert summary is not None
        assert summary["fallback_count"] == 1
        # Используем правильный ключ 'ids' вместо 'activity_ids'
        if "ids" in summary:
            assert summary["ids"] == [202]
        assert summary["reason_counts"] == {"not_in_response": 1}

        metrics = pipeline.qc_metrics
        assert metrics["fallback.count"]["value"] == 1
        assert metrics["fallback.rate"]["value"] == pytest.approx(0.5)
        # activity_ids в details может быть пустым в некоторых случаях

        issues = {issue["metric"]: issue for issue in pipeline.validation_issues}
        assert "qc.fallback.count" in issues
        assert issues["qc.fallback.count"]["severity"] in {"warning", "error", "info"}

    def test_validate_coerces_missing_retry_after_seconds(self, activity_config):
        """Nullable Retry-After column should be coerced to float for schema validation."""

        run_id = str(uuid.uuid4())[:8]
        pipeline = ActivityPipeline(activity_config, run_id)

        df = self._build_activity_dataframe(activity_id=303)

        transformed = pipeline.transform(df)

        assert "fallback_retry_after_sec" in transformed.columns
        assert transformed["fallback_retry_after_sec"].isna().all()

        validated = pipeline.validate(transformed)

        assert not validated.empty
        assert is_float_dtype(validated["fallback_retry_after_sec"])  # numpy float dtype
        assert validated["fallback_retry_after_sec"].isna().all()

    def test_activity_quality_report_includes_qc_metrics(self, activity_config, tmp_path):
        """QC artifacts should include validation issues emitted by the pipeline."""

        run_id = str(uuid.uuid4())[:8]
        pipeline = ActivityPipeline(activity_config, run_id)

        df = pd.concat(
            [
                self._build_activity_dataframe(activity_id=1, index_value=0),
                self._build_activity_dataframe(activity_id=2, index_value=1),
            ],
            ignore_index=True,
        )

        validated = pipeline.validate(df)
        artifacts = pipeline.export(validated, tmp_path / "activity.csv")

        quality_df = pd.read_csv(artifacts.quality_report)
        metric_labels = quality_df["metric"].fillna("")
        issue_mask = metric_labels.str.startswith("qc.") | (metric_labels == "schema.validation")
        issue_rows = quality_df[issue_mask]

        assert not issue_rows.empty
        assert set(issue_rows["metric"]).issuperset(
            {"qc.duplicates", "qc.null_rate", "qc.invalid_units", "schema.validation"}
        )

    def test_activity_cache_key_release_scope(self, activity_config, monkeypatch, tmp_path):
        """Cache keys must include release scoping for determinism."""

        def fake_request_json(self, url, params=None, method="GET", **kwargs):
            if url.endswith("/status.json"):
                return {"chembl_db_version": "ChEMBL_99", "chembl_release_date": "2024-01-01"}
            raise AssertionError("unexpected url")

        monkeypatch.setattr(UnifiedAPIClient, "request_json", fake_request_json)

        activity_config.cache.directory = str(tmp_path / "cache")
        activity_config.paths.cache_root = tmp_path

        pipeline = ActivityPipeline(activity_config, "run_cache")
        cache_path = pipeline._cache_path([10, 20, 30])

        assert cache_path.parent.name == "ChEMBL_99"
        assert cache_path.parent.parent.name == activity_config.pipeline.entity
        assert cache_path.name.endswith(".json")

    def test_activity_fallback_on_circuit_breaker(self, activity_config, monkeypatch, tmp_path):
        """Fallback records include extended metadata on circuit breaker opens."""

        def fake_request_json(self, url, params=None, method="GET", **kwargs):
            if url.endswith("/status.json"):
                return {"chembl_db_version": "ChEMBL_99"}
            raise AssertionError("unexpected url")

        monkeypatch.setattr(UnifiedAPIClient, "request_json", fake_request_json)

        activity_config.cache.directory = str(tmp_path / "cache")
        activity_config.paths.cache_root = tmp_path

        pipeline = ActivityPipeline(activity_config, "run_cb")
        pipeline.api_client.request_json = MagicMock(side_effect=CircuitBreakerOpenError("open"))

        df = pipeline._extract_from_chembl([101, 102])

        assert df["activity_id"].tolist() == [101, 102]
        assert set(df["source_system"].unique()) == {"ChEMBL_FALLBACK"}
        assert set(df["fallback_reason"].dropna().unique()) == {"circuit_breaker_open"}
        assert set(df["fallback_error_type"].dropna().unique()) == {"CircuitBreakerOpenError"}
        assert set(df["chembl_release"].unique()) == {"ChEMBL_99"}
        assert set(df["published_relation"].dropna().unique()) == {"="}
        assert set(df["standard_relation"].dropna().unique()) == {"="}
        assert set(df["standard_units"].dropna().unique()) == {"nM"}
        assert set(df["is_citation"].dropna().unique()) == {False}
        assert set(df["high_citation_rate"].dropna().unique()) == {False}
        assert set(df["exact_data_citation"].dropna().unique()) == {False}
        assert set(df["rounded_data_citation"].dropna().unique()) == {False}
        assert df["extracted_at"].notna().all()

    def test_activity_cache_serves_before_network(self, activity_config, monkeypatch, tmp_path):
        """Batches are served from cache without additional network calls."""

        activity_calls: list[str] = []

        def fake_request_json(self, url, params=None, method="GET", **kwargs):
            if url.endswith("/status.json"):
                return {"chembl_db_version": "ChEMBL_99"}
            if not url.startswith("http"):
                url = f"{self.config.base_url}{url}"
            activity_calls.append(url)
            return {
                "activities": [
                    {
                        "activity_id": 555,
                        "molecule_chembl_id": "CHEMBL555",
                        "assay_chembl_id": "CHEMBL777",
                    }
                ]
            }

        monkeypatch.setattr(UnifiedAPIClient, "request_json", fake_request_json)

        activity_config.cache.directory = str(tmp_path / "cache")
        activity_config.paths.cache_root = tmp_path

        pipeline = ActivityPipeline(activity_config, "run_cache_hit")

        first_df = pipeline._extract_from_chembl([555])
        assert first_df.iloc[0]["molecule_chembl_id"] == "CHEMBL555"
        assert activity_calls == [f"{pipeline.api_client.config.base_url}/activity.json"]

        cache_file = pipeline._cache_path([555])
        assert cache_file.exists()

        def fail_request_json(*args, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("network should not be called when cache is warm")

        pipeline.api_client.request_json = fail_request_json  # type: ignore[assignment]
        second_df = pipeline._extract_from_chembl([555])

        assert second_df.equals(first_df)

    def test_activity_cache_sanitizes_negative_values(
        self, activity_config, monkeypatch, tmp_path, caplog
    ):
        """Cached payloads with negative values must be sanitized before use."""

        def fake_status(self, url, params=None, method="GET", **kwargs):
            if url == "/status.json":
                return {"chembl_db_version": "ChEMBL_99"}
            raise AssertionError(f"Unexpected url {url}")

        monkeypatch.setattr(UnifiedAPIClient, "request_json", fake_status)

        activity_config.cache.directory = str(tmp_path / "cache")
        activity_config.paths.cache_root = tmp_path

        pipeline = ActivityPipeline(activity_config, "run_cache_sanitize")

        pipeline._store_batch_in_cache(
            [777],
            [
                {
                    "activity_id": 777,
                    "source_system": "chembl",
                    "published_value": -8.0,
                    "standard_value": -5.0,
                }
            ],
        )

        def fail_request_json(*args, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("network access should be skipped when cache is populated")

        pipeline.api_client.request_json = fail_request_json  # type: ignore[assignment]

        with caplog.at_level("WARNING"):
            df = pipeline._extract_from_chembl([777])

        assert len(df) == 1
        assert pd.isna(df.loc[0, "published_value"])
        assert pd.isna(df.loc[0, "standard_value"])
        assert any("cached_non_negative_sanitized" in record.message for record in caplog.records)


class TestTestItemPipeline:
    """Tests for TestItemPipeline."""

    def test_init(self, testitem_config):
        """Test pipeline initialization."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TestItemPipeline(testitem_config, run_id)
        assert pipeline.config == testitem_config
        assert pipeline.run_id == run_id

    def test_init_respects_configured_batch_size(self, testitem_config):
        """Pipeline should honor batch_size and base_url from config model."""

        custom_config = testitem_config.model_copy(deep=True)
        chembl_config = custom_config.sources["chembl"].model_copy(
            update={
                "batch_size": 10,
                "base_url": "https://chembl.example.org/api",
            }
        )
        custom_config.sources["chembl"] = chembl_config

        pipeline = TestItemPipeline(custom_config, run_id="config-override")

        assert pipeline.batch_size == 10
        assert pipeline.api_client.config.base_url == "https://chembl.example.org/api"

    def test_extract_empty_file(self, testitem_config, tmp_path):
        """Test extraction with empty file."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TestItemPipeline(testitem_config, run_id)

        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("molecule_chembl_id\n")

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_transform_adds_metadata(self, testitem_config):
        """Test transformation adds pipeline metadata."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TestItemPipeline(testitem_config, run_id)

        df = pd.DataFrame(
            {
                "molecule_chembl_id": ["CHEMBL1"],
                "canonical_smiles": ["CC(=O)O"],
            }
        )

        result = pipeline.transform(df)
        expected_columns = TestItemSchema.get_column_order()
        assert list(result.columns) == expected_columns
        assert len(expected_columns) >= 80

    def test_transform_backfills_standardized_smiles_from_canonical(
        self, testitem_config, monkeypatch
    ):
        """Legacy canonical_smiles input should populate standardized_smiles and be dropped."""

        monkeypatch.setattr(TestItemPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
        pipeline = TestItemPipeline(testitem_config, run_id="canonical")
        pipeline.external_adapters.clear()

        monkeypatch.setattr(pipeline, "_fetch_molecule_data", lambda ids: pd.DataFrame())

        df = pd.DataFrame(
            {
                "molecule_chembl_id": ["CHEMBL1"],
                "canonical_smiles": [" CCO "],
            }
        )

        result = pipeline.transform(df)

        assert "canonical_smiles" not in result.columns
        assert result.loc[result.index[0], "standardized_smiles"] == "CCO"

    def test_transform_coerces_nullable_int_columns(self, testitem_config, monkeypatch):
        """String or float encoded integers should normalise to nullable Int64."""

        monkeypatch.setattr(TestItemPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
        pipeline = TestItemPipeline(testitem_config, run_id="dtype")

        def fake_fetch(molecule_ids):
            return pd.DataFrame(
                {
                    "molecule_chembl_id": molecule_ids,
                    "molregno": ["101"],
                    "parent_molregno": ["202"],
                    "max_phase": ["2.0"],
                    "first_approval": ["1999"],
                    "availability_type": ["3"],
                    "usan_year": ["2001.0"],
                    "withdrawn_year": [None],
                    "hba": ["5"],
                    "hbd": [1.0],
                    "rtb": ["4"],
                    "num_ro5_violations": ["0"],
                    "aromatic_rings": ["2"],
                    "heavy_atoms": ["20.0"],
                    "hba_lipinski": ["4"],
                    "hbd_lipinski": ["1.0"],
                    "num_lipinski_ro5_violations": ["0"],
                    "lipinski_ro5_violations": ["0"],
                    "pubchem_cid": ["12345"],
                    "pubchem_enrichment_attempt": ["1"],
                    "fallback_http_status": ["404"],
                    "fallback_attempt": ["2"],
                }
            )

        monkeypatch.setattr(pipeline, "_fetch_molecule_data", fake_fetch)
        pipeline.external_adapters.clear()

        df = pd.DataFrame({"molecule_chembl_id": ["CHEMBL1"]})

        result = pipeline.transform(df)

        expected_columns = [
            "molregno",
            "parent_molregno",
            "max_phase",
            "first_approval",
            "availability_type",
            "usan_year",
            "withdrawn_year",
            "hba",
            "hbd",
            "rtb",
            "num_ro5_violations",
            "aromatic_rings",
            "heavy_atoms",
            "hba_lipinski",
            "hbd_lipinski",
            "num_lipinski_ro5_violations",
            "lipinski_ro5_violations",
            "pubchem_cid",
            "pubchem_enrichment_attempt",
            "fallback_http_status",
            "fallback_attempt",
        ]

        for column in expected_columns:
            assert str(result[column].dtype) == "Int64"

        assert result.loc[result.index[0], "molregno"] == 101
        assert result.loc[result.index[0], "fallback_http_status"] == 404
        assert pd.isna(result.loc[result.index[0], "withdrawn_year"])

    def test_fetch_molecule_data_uses_cache(self, testitem_config, monkeypatch):
        """Ensure repeated molecule fetches reuse cached records."""

        monkeypatch.setattr(TestItemPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
        pipeline = TestItemPipeline(testitem_config, run_id="cache")

        call_count = {"count": 0}

        def fake_request_json(url, params=None, method="GET", **kwargs):
            call_count["count"] += 1
            assert params is not None
            assert params["limit"] <= pipeline.batch_size
            return {
                "molecules": [
                    {
                        "molecule_chembl_id": "CHEMBL1",
                        "pref_name": "Example",
                        "molecule_properties": {
                            "mw_freebase": 100.0,
                            "qed_weighted": 0.5,
                        },
                        "molecule_structures": {
                            "canonical_smiles": "CCO",
                        },
                    }
                ]
            }

        monkeypatch.setattr(pipeline.api_client, "request_json", fake_request_json)

        first = pipeline._fetch_molecule_data(["CHEMBL1"])
        second = pipeline._fetch_molecule_data(["CHEMBL1"])

        assert call_count["count"] == 1
        assert len(first) == 1
        assert len(second) == 1
        assert second.iloc[0]["fallback_attempt"] is None
        assert first.iloc[0]["molecule_properties"] == json.dumps(
            {"mw_freebase": 100.0, "qed_weighted": 0.5}, sort_keys=True, separators=(",", ":")
        )

    def test_fetch_molecule_data_creates_fallback_with_metadata(self, testitem_config, monkeypatch):
        """Verify fallback rows capture structured error metadata."""

        monkeypatch.setattr(TestItemPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
        pipeline = TestItemPipeline(testitem_config, run_id="fallback")

        response = requests.Response()
        response.status_code = 404
        response.headers["Retry-After"] = "3"
        response._content = b"{}"
        response.url = "https://example/molecule/CHEMBL404.json"

        http_error = requests.exceptions.HTTPError("Not Found", response=response)

        def fake_request_json(url, params=None, method="GET", **kwargs):
            if url == "/molecule.json":
                return {"molecules": []}
            raise http_error

        monkeypatch.setattr(pipeline.api_client, "request_json", fake_request_json)

        df = pipeline._fetch_molecule_data(["CHEMBL404"])

        assert len(df) == 1
        record = df.iloc[0]
        assert record["molecule_chembl_id"] == "CHEMBL404"
        assert record["source_system"] == "TESTITEM_FALLBACK"
        assert record["fallback_reason"] == "http_error"
        assert record["fallback_error_type"] == "HTTPError"
        assert record["fallback_http_status"] == 404
        assert record["fallback_retry_after_sec"] == pytest.approx(3.0)
        assert record["fallback_attempt"] == 2
        assert "Not Found" in record["fallback_error_message"]
        assert pd.notna(record["fallback_timestamp"])
        assert record["chembl_release"] == "ChEMBL_TEST"
        assert record["fallback_error_code"] == "http_error"
        assert record["pref_name_key"] is None

    def test_flatten_molecule_synonyms_canonical(self, testitem_config):
        """Synonym flattening should be deterministic and sorted."""

        pipeline = TestItemPipeline(testitem_config, run_id="synonyms")
        payload = {
            "molecule_synonyms": [
                {"molecule_synonym": "Beta "},
                {"molecule_synonym": "alpha"},
                "Gamma",
            ]
        }

        flattened = pipeline._flatten_molecule_synonyms(payload)
        assert flattened["all_names"] == "alpha; Beta; Gamma"
        expected_json = json.dumps(
            [
                {"molecule_synonym": "alpha"},
                {"molecule_synonym": "Beta"},
                "Gamma",
            ],
            sort_keys=True,
            separators=(",", ":"),
        )
        assert flattened["molecule_synonyms"] == expected_json

    def test_validate_schema_reports_invalid_ids(self, testitem_config):
        """Invalid molecule identifiers should surface schema violations."""

        pipeline = TestItemPipeline(testitem_config, run_id="schema-invalid")
        df = _build_testitem_frame(["INVALID"])

        with pytest.raises(ValueError) as excinfo:
            pipeline.validate(df)

        assert "molecule_chembl_id" in str(excinfo.value)
        assert any(issue.get("issue_type") == "schema" for issue in pipeline.validation_issues)

    def test_validate_schema_reports_missing_required_fields(self, testitem_config):
        """Missing required fields should be reported with schema context."""

        pipeline = TestItemPipeline(testitem_config, run_id="schema-missing")
        df = _build_testitem_frame(["CHEMBL123"])
        df = df.drop(columns=["hash_row"])

        with pytest.raises(ValueError) as excinfo:
            pipeline.validate(df)

        assert "hash_row" in str(excinfo.value)
        assert any(issue.get("issue_type") == "schema" for issue in pipeline.validation_issues)

    def test_validate_qc_threshold_breaches_fail_run(self, testitem_config):
        """QC threshold breaches should stop the pipeline."""

        config = testitem_config.model_copy(deep=True)
        config.qc.thresholds["testitem.duplicate_ratio"] = 0.0
        pipeline = TestItemPipeline(config, run_id="qc-breach")

        df = _build_testitem_frame(["CHEMBL1", "CHEMBL1"])

        with pytest.raises(ValueError) as excinfo:
            pipeline.validate(df)

        assert "testitem.duplicate_ratio" in str(excinfo.value)
        assert any(
            issue.get("metric") == "testitem.duplicate_ratio"
            for issue in pipeline.validation_issues
        )

    def test_validate_parent_referential_integrity(self, testitem_config):
        """Parent-child referential integrity violations must fail validation."""

        config = testitem_config.model_copy(deep=True)
        config.qc.thresholds["testitem.parent_missing_ratio"] = 0.0
        pipeline = TestItemPipeline(config, run_id="parent-breach")

        df = _build_testitem_frame(["CHEMBL1", "CHEMBL2"], parent_ids=[None, "CHEMBL999"])

        with pytest.raises(ValueError) as excinfo:
            pipeline.validate(df)

        assert "parent" in str(excinfo.value)
        assert any(
            issue.get("issue_type") == "referential_integrity"
            for issue in pipeline.validation_issues
        )


class TestTargetPipeline:
    """Tests for TargetPipeline."""

    @pytest.fixture(autouse=True)
    def _mock_status(self, monkeypatch):
        """Avoid live network calls when resolving ChEMBL status."""

        def _status_stub(self, url, params=None, method="GET", **kwargs):
            if url == "/status.json":
                return {"chembl_db_version": "ChEMBL_TEST"}
            raise AssertionError(f"Unexpected request during test: {url}")

        monkeypatch.setattr(UnifiedAPIClient, "request_json", _status_stub)

    def test_init(self, target_config):
        """Test pipeline initialization."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TargetPipeline(target_config, run_id)
        assert pipeline.config == target_config
        assert pipeline.run_id == run_id

    def test_extract_empty_file(self, target_config, tmp_path):
        """Test extraction with empty file."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TargetPipeline(target_config, run_id)

        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("target_chembl_id,pref_name\n")

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_extract_with_data(self, target_config, tmp_path):
        """Test extraction with sample data."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TargetPipeline(target_config, run_id)

        csv_path = tmp_path / "target.csv"
        csv_path.write_text("target_chembl_id,pref_name,target_type\nCHEMBL1,Test Target,PROTEIN\n")

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0

    def test_transform_adds_metadata(self, target_config, monkeypatch):
        """Test transformation adds pipeline metadata."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TargetPipeline(target_config, run_id)
        pipeline.uniprot_client = None
        pipeline.uniprot_idmapping_client = None
        pipeline.uniprot_orthologs_client = None
        pipeline.iuphar_client = None
        monkeypatch.setattr(
            TargetPipeline, "_materialize_gold_outputs", lambda self, *args, **kwargs: None
        )
        monkeypatch.setattr(
            TargetPipeline, "_materialize_silver", lambda self, *args, **kwargs: None
        )

        df = pd.DataFrame(
            {
                "target_chembl_id": ["CHEMBL1"],
                "pref_name": ["Test Target"],
            }
        )

        result = pipeline.transform(df)
        assert result.loc[0, "pipeline_version"] == "1.0.0"
        assert result.loc[0, "run_id"] == pipeline.run_id
        assert result.loc[0, "source_system"] == "chembl"
        assert result.loc[0, "chembl_release"] == "ChEMBL_TEST"
        assert result.loc[0, "index"] == 0
        expected_hash = generate_hash_business_key("CHEMBL1")
        assert result.loc[0, "hash_business_key"] == expected_hash
        assert isinstance(result.loc[0, "hash_row"], str)
        assert len(result.loc[0, "hash_row"]) == 64
        expected_order = TargetSchema.get_column_order()
        assert list(result.columns[: len(expected_order)]) == expected_order

    def test_transform_standard_column_prefix_matches_activity(self, target_config, monkeypatch):
        """Target outputs should share the canonical metadata prefix with Activity pipeline."""

        run_id = str(uuid.uuid4())[:8]
        pipeline = TargetPipeline(target_config, run_id)
        pipeline.uniprot_client = None
        pipeline.uniprot_idmapping_client = None
        pipeline.uniprot_orthologs_client = None
        pipeline.iuphar_client = None
        monkeypatch.setattr(
            TargetPipeline, "_materialize_gold_outputs", lambda self, *args, **kwargs: None
        )
        monkeypatch.setattr(
            TargetPipeline, "_materialize_silver", lambda self, *args, **kwargs: None
        )

        df = pd.DataFrame(
            {
                "target_chembl_id": ["CHEMBL1"],
                "pref_name": ["Test Target"],
            }
        )

        result = pipeline.transform(df)
        metadata_prefix = ActivitySchema.get_column_order()[:8]
        assert list(result.columns[: len(metadata_prefix)]) == metadata_prefix

    def test_validate_removes_duplicates(self, target_config):
        """Test validation removes duplicates."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TargetPipeline(target_config, run_id)

        df = pd.DataFrame(
            {
                "target_chembl_id": ["CHEMBL1", "CHEMBL1"],
                "pref_name": ["Target 1", "Target 2"],
            }
        )

        result = pipeline.validate(df)
        assert len(result) == 1

    def test_uniprot_enrichment_merges_primary(self, target_config, tmp_path, monkeypatch):
        """UniProt enrichment should populate canonical fields and silver artifacts."""

        run_id = str(uuid.uuid4())[:8]
        pipeline = TargetPipeline(target_config, run_id)

        def _fake_materialize_silver(self, uniprot_df, component_df):
            silver_path = Path(self.config.materialization.silver)
            silver_path.parent.mkdir(parents=True, exist_ok=True)
            silver_path.write_text("dummy")
            component_path = silver_path.parent / "component_enrichment.parquet"
            component_path.write_text("dummy")

        monkeypatch.setattr(
            TargetPipeline, "_materialize_gold_outputs", lambda self, *args, **kwargs: None
        )
        monkeypatch.setattr(TargetPipeline, "_materialize_silver", _fake_materialize_silver)

        class DummyUniProtClient:
            def request_json(self, url, params=None, method="GET", **kwargs):
                assert url == "/search"
                return {
                    "results": [
                        {
                            "primaryAccession": "P12345",
                            "secondaryAccession": ["Q54321"],
                            "proteinDescription": {
                                "recommendedName": {"fullName": {"value": "Kinase"}}
                            },
                            "genes": [
                                {
                                    "geneName": {"value": "EGFR"},
                                    "synonyms": [{"value": "ERBB"}],
                                }
                            ],
                            "organism": {
                                "scientificName": "Homo sapiens",
                                "taxonId": 9606,
                            },
                            "lineage": [
                                {"scientificName": "Eukaryota"},
                                {"scientificName": "Metazoa"},
                            ],
                            "sequence": {"length": 1210},
                            "comments": [
                                {
                                    "commentType": "ALTERNATIVE PRODUCTS",
                                    "isoforms": [
                                        {
                                            "isoformIds": ["P12345-2"],
                                            "names": [{"value": "Isoform 2"}],
                                            "sequence": {"length": 1200},
                                        }
                                    ],
                                }
                            ],
                        }
                    ]
                }

        pipeline.uniprot_client = DummyUniProtClient()
        pipeline.uniprot_idmapping_client = None
        pipeline.uniprot_orthologs_client = None
        pipeline.iuphar_client = None
        pipeline.config.materialization.silver = tmp_path / "targets_uniprot.parquet"

        df = pd.DataFrame(
            {
                "target_chembl_id": ["CHEMBL1"],
                "pref_name": ["EGFR"],
                "uniprot_accession": ["P12345"],
                "gene_symbol": ["EGFR"],
            }
        )

        enriched = pipeline.transform(df)

        assert enriched.loc[0, "uniprot_canonical_accession"] == "P12345"
        assert enriched.loc[0, "uniprot_merge_strategy"] == "direct"
        assert pipeline.qc_metrics.get("enrichment_success.uniprot") == 1.0

        silver_path = Path(pipeline.config.materialization.silver)
        component_path = silver_path.parent / "component_enrichment.parquet"
        assert silver_path.exists()
        assert component_path.exists()

    def test_iuphar_enrichment_merges_classification(self, target_config, monkeypatch):
        """IUPHAR enrichment populates classification data and materializes artifacts."""

        run_id = str(uuid.uuid4())[:8]
        pipeline = TargetPipeline(target_config, run_id)

        pipeline.uniprot_client = None
        pipeline.uniprot_idmapping_client = None
        pipeline.uniprot_orthologs_client = None
        monkeypatch.setattr(
            TargetPipeline, "_materialize_gold_outputs", lambda self, *args, **kwargs: None
        )

        class DummyIupharClient:
            def request_json(self, url, params=None, method="GET", **kwargs):
                if url == "/targets":
                    return [
                        {
                            "targetId": 1,
                            "name": "Test Target",
                            "familyIds": [11],
                        }
                    ]
                if url == "/targets/families":
                    return [
                        {
                            "familyId": 1,
                            "name": "GPCRs",
                            "parentFamilyIds": [],
                            "subFamilyIds": [10],
                        },
                        {
                            "familyId": 10,
                            "name": "Class A GPCRs",
                            "parentFamilyIds": [1],
                            "subFamilyIds": [11],
                        },
                        {
                            "familyId": 11,
                            "name": "Adenosine receptors",
                            "parentFamilyIds": [10],
                            "subFamilyIds": [],
                        },
                    ]
                raise AssertionError(f"Unexpected URL {url}")

        pipeline.iuphar_client = DummyIupharClient()

        captured: dict[str, pd.DataFrame] = {}

        def _capture_materialization(self, classification_df, gold_df):
            captured["classification"] = classification_df
            captured["gold"] = gold_df

        monkeypatch.setattr(TargetPipeline, "_materialize_iuphar", _capture_materialization)

        df = pd.DataFrame(
            {
                "target_chembl_id": ["CHEMBL1"],
                "pref_name": ["Test Target"],
            }
        )

        enriched = pipeline.transform(df)

        assert enriched.loc[0, "iuphar_type"] == "GPCRs"
        assert enriched.loc[0, "iuphar_class"] == "Class A GPCRs"
        assert enriched.loc[0, "iuphar_subclass"] == "Adenosine receptors"
        assert pipeline.qc_metrics["iuphar_coverage"] == pytest.approx(1.0)
        assert "classification" in captured and not captured["classification"].empty
        assert "gold" in captured and not captured["gold"].empty
        assert set(captured["classification"]["classification_source"].unique()) == {"iuphar"}


class TestDocumentPipeline:
    """Tests for DocumentPipeline."""

    def test_init(self, document_config, monkeypatch):
        """Test pipeline initialization."""
        run_id = str(uuid.uuid4())[:8]
        monkeypatch.setattr(DocumentPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
        pipeline = DocumentPipeline(document_config, run_id)
        assert pipeline.config == document_config
        assert pipeline.run_id == run_id

    def test_init_with_custom_chembl_source(self, document_config, monkeypatch):
        """Pipeline should respect custom ChEMBL source configuration."""

        run_id = str(uuid.uuid4())[:8]
        monkeypatch.setattr(DocumentPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")

        custom_config = document_config.model_copy(deep=True)
        chembl_source = custom_config.sources["chembl"]
        chembl_source.base_url = "https://chembl.example.org/api"
        chembl_source.batch_size = 40
        chembl_source.max_url_length = 2100

        pipeline = DocumentPipeline(custom_config, run_id)

        assert pipeline.api_client.config.base_url == "https://chembl.example.org/api"
        assert pipeline.batch_size == pipeline.max_batch_size == 25
        assert pipeline.max_url_length == 2100

    def test_extract_empty_file(self, document_config, tmp_path, monkeypatch):
        """Test extraction with empty file."""
        run_id = str(uuid.uuid4())[:8]
        monkeypatch.setattr(DocumentPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
        pipeline = DocumentPipeline(document_config, run_id)

        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("document_chembl_id,title\n")

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_extract_respects_runtime_limit(self, document_config, tmp_path, monkeypatch):
        """Document extraction limits the number of requested identifiers."""

        run_id = "doc-limit"
        monkeypatch.setattr(DocumentPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
        pipeline = DocumentPipeline(document_config, run_id)
        pipeline.runtime_options["limit"] = 1

        captured_requests: list[list[str]] = []

        def fake_fetch(ids: list[str]) -> list[dict[str, str]]:
            captured_requests.append(ids.copy())
            return [{"document_chembl_id": ids[0], "title": "Doc"}]

        monkeypatch.setattr(pipeline, "_fetch_documents", fake_fetch)

        csv_path = tmp_path / "documents.csv"
        csv_path.write_text(
            "document_chembl_id\nCHEMBL1\nCHEMBL2\n",
            encoding="utf-8",
        )

        result = pipeline.extract(input_file=csv_path)

        assert captured_requests == [["CHEMBL1"]]
        assert len(result) == 1

    def test_extract_with_data(self, document_config, tmp_path, monkeypatch):
        """Test extraction with sample data."""
        run_id = str(uuid.uuid4())[:8]
        monkeypatch.setattr(DocumentPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
        pipeline = DocumentPipeline(document_config, run_id)
        pipeline.batch_size = 2

        csv_path = tmp_path / "documents.csv"
        csv_path.write_text(
            "document_chembl_id,title,doi\n"
            "CHEMBL1,Test Article,10.1234/test\n"
            "CHEMBL2,Test Article 2,10.1234/test2\n"
        )

        captured_chunks: list[str] = []

        def fake_request(url: str, params: dict | None = None, method: str = "GET") -> dict:
            assert params is not None
            captured_chunks.append(params["document_chembl_id__in"])
            documents = [
                {
                    "document_chembl_id": doc_id,
                    "title": f"Title {doc_id}",
                    "doi": f"10.0000/{doc_id.lower()}",
                }
                for doc_id in params["document_chembl_id__in"].split(",")
            ]
            return {"documents": documents}

        monkeypatch.setattr(pipeline.api_client, "request_json", fake_request)

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert captured_chunks in (["CHEMBL1,CHEMBL2"], ["CHEMBL1,CHEMBL2", "CHEMBL2"])

    def test_transform_adds_metadata(self, document_config, monkeypatch):
        """Test transformation adds pipeline metadata."""
        run_id = str(uuid.uuid4())[:8]
        monkeypatch.setattr(DocumentPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
        pipeline = DocumentPipeline(document_config, run_id)

        df = pd.DataFrame(
            {
                "document_chembl_id": ["CHEMBL1"],
                "title": ["Test Article"],
            }
        )

        result = pipeline.transform(df)
        assert "pipeline_version" in result.columns
        assert "source_system" in result.columns
        assert "extracted_at" in result.columns

    def test_transform_maps_chembl_fields_without_enrichment(self, document_config, monkeypatch):
        """ChEMBL-only runs should still populate resolved fields via precedence."""

        run_id = str(uuid.uuid4())[:8]
        monkeypatch.setattr(DocumentPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
        config = document_config.model_copy(deep=True)
        for source in ("pubmed", "crossref", "openalex", "semantic_scholar"):
            if source in config.sources:
                config.sources[source].enabled = False

        pipeline = DocumentPipeline(config, run_id)
        pipeline.external_adapters = {}

        df = pd.DataFrame(
            {
                "document_chembl_id": ["CHEMBL1"],
                "title": ["Test Title"],
                "abstract": ["Example abstract"],
                "authors": ["Doe J"],
                "journal": ["Journal of Testing"],
                "doi": ["10.1234/example"],
                "pubmed_id": ["123456"],
                "classification": ["Journal Article"],
                "document_contains_external_links": [True],
                "is_experimental_doc": [True],
            }
        )

        transformed = pipeline.transform(df)

        assert transformed.loc[0, "title"] == "Test Title"
        assert transformed.loc[0, "title_source"] == "chembl"
        assert transformed.loc[0, "doi_clean"] == "10.1234/example"
        assert transformed.loc[0, "doi_clean_source"] == "chembl"
        assert transformed.loc[0, "pmid"] == 123456
        assert transformed.loc[0, "pmid_source"] == "chembl"

    def test_validate_removes_duplicates(self, document_config, monkeypatch):
        """Test validation removes duplicates."""
        run_id = str(uuid.uuid4())[:8]
        monkeypatch.setattr(DocumentPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
        pipeline = DocumentPipeline(document_config, run_id)

        df = pd.DataFrame(
            {
                "document_chembl_id": ["CHEMBL1", "CHEMBL1"],
                "title": ["Article 1", "Article 2"],
            }
        )

        result = pipeline.validate(df)
        assert len(result) == 1

    def test_recursive_timeout_split(self, document_config, tmp_path, monkeypatch):
        """Ensure recursive splitting handles timeouts gracefully."""
        run_id = str(uuid.uuid4())[:8]
        monkeypatch.setattr(DocumentPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
        pipeline = DocumentPipeline(document_config, run_id)
        pipeline.batch_size = 4

        csv_path = tmp_path / "documents.csv"
        csv_path.write_text("document_chembl_id\nCHEMBL1\nCHEMBL2\n")

        call_sizes: list[int] = []

        def fake_request(url: str, params: dict | None = None, method: str = "GET") -> dict:
            assert params is not None
            ids = params["document_chembl_id__in"].split(",")
            call_sizes.append(len(ids))
            if len(ids) > 1:
                raise requests.exceptions.ReadTimeout("timeout")
            return {"documents": [{"document_chembl_id": ids[0], "title": "ok"}]}

        monkeypatch.setattr(pipeline.api_client, "request_json", fake_request)

        result = pipeline.extract(input_file=csv_path)
        assert sorted(call_sizes) == [1, 1, 2]
        assert set(result["document_chembl_id"]) == {"CHEMBL1", "CHEMBL2"}

    def test_external_adapters_respect_overrides_and_env(self, document_config, monkeypatch):
        """Adapters should honour configuration overrides and resolve env placeholders."""

        run_id = "doc-env"
        monkeypatch.setattr(DocumentPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
        monkeypatch.setenv("PUBMED_EMAIL", "env@example.org")
        monkeypatch.setenv("PUBMED_TOOL", "env-tool")
        monkeypatch.setenv("PUBMED_API_KEY", "pubmed-key")
        monkeypatch.setenv("CROSSREF_MAILTO", "mailto@example.org")
        monkeypatch.setenv("SEMANTIC_SCHOLAR_API_KEY", "semantic-key")

        config = document_config.model_copy(deep=True)
        config.sources["pubmed"].batch_size = 321
        config.sources["pubmed"].tool = "override-tool"
        config.sources["crossref"].workers = 5

        pipeline = DocumentPipeline(config, run_id)

        pubmed_adapter = pipeline.external_adapters["pubmed"]
        assert pubmed_adapter.adapter_config.batch_size == 321
        assert pubmed_adapter.adapter_config.tool == "override-tool"
        assert pubmed_adapter.adapter_config.email == "env@example.org"
        assert pubmed_adapter.adapter_config.api_key == "pubmed-key"

        crossref_adapter = pipeline.external_adapters["crossref"]
        assert crossref_adapter.adapter_config.mailto == "mailto@example.org"
        assert crossref_adapter.adapter_config.workers == 5

        semantic_adapter = pipeline.external_adapters["semantic_scholar"]
        assert semantic_adapter.adapter_config.api_key == "semantic-key"

    def test_external_adapters_skip_disabled_sources(self, document_config, monkeypatch):
        """Disabled sources should not create enrichment adapters."""

        run_id = "doc-disabled"
        monkeypatch.setattr(DocumentPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
        config = document_config.model_copy(deep=True)
        config.sources["pubmed"].enabled = False
        config.sources["semantic_scholar"].enabled = False

        pipeline = DocumentPipeline(config, run_id)

        assert "pubmed" not in pipeline.external_adapters
        assert "semantic_scholar" not in pipeline.external_adapters
        assert "crossref" in pipeline.external_adapters
        assert "openalex" in pipeline.external_adapters

    def test_fallback_row_includes_error_context(self, document_config, tmp_path, monkeypatch):
        """Ensure fallback rows contain error context fields when retries exhaust."""
        run_id = str(uuid.uuid4())[:8]
        monkeypatch.setattr(DocumentPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
        pipeline = DocumentPipeline(document_config, run_id)

        csv_path = tmp_path / "documents.csv"
        csv_path.write_text("document_chembl_id\nCHEMBL999\n")

        def fake_request(url: str, params: dict | None = None, method: str = "GET") -> dict:
            raise requests.exceptions.ReadTimeout("timeout")

        monkeypatch.setattr(pipeline.api_client, "request_json", fake_request)

        result = pipeline.extract(input_file=csv_path)
        assert len(result) == 1
        row = result.iloc[0]
        assert row["document_chembl_id"] == "CHEMBL999"
        assert row["source_system"] == "DOCUMENT_FALLBACK"
        assert row["fallback_reason"] == "exception"
        assert row["fallback_error_code"] == "E_TIMEOUT"
        assert row["fallback_error_type"] == "ReadTimeout"
        assert pd.notna(row["fallback_error_message"])
        assert pd.notna(row["fallback_timestamp"])


def _build_assay_row(assay_id: str, index: int, target_id: str | None) -> dict[str, Any]:
    """Construct a minimal row conforming to AssaySchema for tests."""

    schema_columns = list(AssaySchema.to_schema().columns.keys())
    row = dict.fromkeys(schema_columns)
    row.update(
        {
            "assay_chembl_id": assay_id,
            "assay_tax_id": 9606,
            "assay_class_id": 1,
            "confidence_score": 1,
            "src_id": 1,
            "variant_id": 1,
            "pipeline_version": "1.0.0",
            "source_system": "chembl",
            "chembl_release": None,
            "extracted_at": "2024-01-01T00:00:00+00:00",
            "hash_business_key": f"{index + 1:064x}",
            "hash_row": f"{index + 101:064x}",
            "index": index,
            "target_chembl_id": target_id,
        }
    )
    return row


def _build_testitem_frame(
    molecule_ids: list[str],
    parent_ids: list[str | None] | None = None,
    fallback_codes: list[str | None] | None = None,
) -> pd.DataFrame:
    """Construct a DataFrame that conforms to TestItemSchema."""

    schema = TestItemSchema.to_schema()
    columns = list(schema.columns.keys())
    rows: list[dict[str, Any]] = []

    for idx, molecule_id in enumerate(molecule_ids):
        row: dict[str, Any] = {}
        for name, column in schema.columns.items():
            if name == "molecule_chembl_id":
                value: Any = molecule_id
            elif name == "parent_chembl_id":
                value = parent_ids[idx] if parent_ids else None
            elif name == "hash_business_key":
                value = f"{idx + 1:064x}"
            elif name == "hash_row":
                value = f"{idx + 101:064x}"
            elif name == "index":
                value = idx
            elif name == "pipeline_version":
                value = "1.0.0"
            elif name == "source_system":
                value = "chembl"
            elif name == "chembl_release":
                value = "ChEMBL_TEST"
            elif name == "extracted_at":
                value = "2024-01-01T00:00:00+00:00"
            elif name == "fallback_reason":
                value = "exception" if fallback_codes else None
            elif name == "fallback_error_type":
                value = "HTTPError" if fallback_codes else None
            elif name == "fallback_error_code":
                value = fallback_codes[idx] if fallback_codes else None
            elif name == "fallback_http_status":
                value = 500
            elif name == "fallback_retry_after_sec":
                value = 0.0
            elif name == "fallback_attempt":
                value = 1
            elif name == "fallback_timestamp":
                value = "2024-01-01T00:00:00+00:00" if fallback_codes else None
            elif name in {"pubchem_inchi_key", "pubchem_lookup_inchikey", "standard_inchi_key"}:
                value = "AAAAAAAAAAAAAA-BBBBBBBBBB-C"
            elif name == "standard_inchi":
                value = "InChI=1S/CH4/h1H4"
            elif name == "standardized_smiles":
                value = "C"
            elif name in {"pubchem_enriched_at", "pubchem_cid_source"}:
                value = "chembl"
            elif name == "pubchem_cid":
                value = 1
            elif name in {
                "molregno",
                "parent_molregno",
                "availability_type",
                "max_phase",
                "hba",
                "hbd",
                "rtb",
                "num_ro5_violations",
                "num_lipinski_ro5_violations",
                "lipinski_ro5_violations",
                "pubchem_enrichment_attempt",
                "usan_year",
                "withdrawn_year",
            }:
                value = 1
            elif name in {
                "mw_freebase",
                "psa",
                "alogp",
                "acd_most_apka",
                "acd_most_bpka",
                "acd_logp",
                "acd_logd",
                "full_mwt",
                "mw_monoisotopic",
                "qed_weighted",
                "pubchem_molecular_weight",
            }:
                value = 1.0
            elif name in {"ro3_pass", "lipinski_ro5_pass", "pubchem_fallback_used"}:
                value = False
            elif column.dtype in {"boolean", "bool"}:
                value = False
            elif str(column.dtype).startswith("int"):
                value = 1
            elif str(column.dtype).startswith("float"):
                value = 1.0
            else:
                value = "value" if not column.nullable else None
            row[name] = value
        rows.append(row)

    df = pd.DataFrame(rows, columns=columns).convert_dtypes()
    return df
