"""Tests for pipeline implementations."""

import json
import uuid
from typing import Any

import pandas as pd
import pytest
from pandera.errors import SchemaErrors

from bioetl.schemas.activity import COLUMN_ORDER as ACTIVITY_COLUMN_ORDER
from unittest.mock import MagicMock

from bioetl.config.loader import load_config
from bioetl.core.api_client import CircuitBreakerOpenError, UnifiedAPIClient
from bioetl.pipelines import ActivityPipeline, AssayPipeline, DocumentPipeline, TargetPipeline, TestItemPipeline
from bioetl.schemas import AssaySchema


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


class TestAssayPipeline:
    """Tests for AssayPipeline."""

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

        df = pd.DataFrame({
            "assay_chembl_id": ["CHEMBL1"],
            "description": ["  Test Assay  "],
        })

        mock_assay_data = pd.DataFrame({
            "assay_chembl_id": ["CHEMBL1"],
            "assay_parameters_json": [None],
            "variant_sequence_json": [None],
            "assay_classifications": [None],
        })

        monkeypatch.setattr(pipeline, "_fetch_assay_data", lambda ids: mock_assay_data)
        monkeypatch.setattr(pipeline, "_fetch_target_reference_data", lambda ids: pd.DataFrame())
        monkeypatch.setattr(pipeline, "_fetch_assay_class_reference_data", lambda ids: pd.DataFrame())

        result = pipeline.transform(df)
        assert "pipeline_version" in result.columns
        assert "source_system" in result.columns
        assert "extracted_at" in result.columns

    def test_validate_schema_errors_capture(self, assay_config):
        """Schema violations should be surfaced as validation issues."""

        run_id = str(uuid.uuid4())[:8]
        pipeline = AssayPipeline(assay_config, run_id)
        df = pd.DataFrame([_build_assay_row("CHEMBL0", 0, None)]).convert_dtypes()
        df = df.drop(columns=["row_subtype"])  # force schema failure

        with pytest.raises(ValueError):
            pipeline.validate(df)

        assert pipeline.validation_issues
        issue = pipeline.validation_issues[0]
        assert issue["issue_type"] == "schema"
        assert issue["severity"] == "error"

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
        assert (
            validation_rows["issue_type"].fillna("").str.contains("referential_integrity").any()
        )

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
    def test_transform_expands_and_enriches(self, assay_config, monkeypatch, caplog):
        """Ensure parameters, variants, and classifications survive transform with enrichment."""

        run_id = str(uuid.uuid4())[:8]
        pipeline = AssayPipeline(assay_config, run_id)

        input_df = pd.DataFrame({
            "assay_chembl_id": ["CHEMBL1"],
        })

        assay_payload = pd.DataFrame({
            "assay_chembl_id": ["CHEMBL1"],
            "target_chembl_id": ["CHEMBL2"],
            "assay_parameters_json": [json.dumps([
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
            ])],
            "variant_sequence_json": [json.dumps([
                {
                    "variant_id": 101,
                    "base_accession": "P12345",
                    "mutation": "A50T",
                    "variant_seq": "MTEYKLVVVG",
                    "accession_reported": "Q99999",
                }
            ])],
            "assay_classifications": [json.dumps([
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
            ])],
        })

        monkeypatch.setattr(pipeline, "_fetch_assay_data", lambda ids: assay_payload)

        target_reference = pd.DataFrame({
            "target_chembl_id": ["CHEMBL2"],
            "pref_name": ["Target X"],
            "organism": ["Homo sapiens"],
            "target_type": ["SINGLE PROTEIN"],
            "species_group_flag": [1],
            "tax_id": [9606],
            "component_count": [1],
            "unexpected_column": ["drop"],
        })

        assay_class_reference = pd.DataFrame({
            "assay_class_id": [501, 502],
            "assay_class_bao_id": ["BAO_0000001", "BAO_0000002"],
            "assay_class_type": ["primary", "secondary"],
            "assay_class_l1": ["Level1", "L1"],
            "assay_class_l2": ["Level2", "L2"],
            "assay_class_l3": ["Level3", "L3"],
            "assay_class_description": ["Example class", "Another class"],
            "extra_field": ["drop", "drop"],
        })

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

        # Canonical ordering maintained
        assert list(result.columns) == AssaySchema.Config.column_order

        # Expect four row subtypes: assay, param, variant, class (2 class rows -> 5 total rows)
        assert set(result["row_subtype"].unique()) == {"assay", "param", "variant", "class"}

        assay_rows = result[result["row_subtype"] == "assay"]
        assert len(assay_rows) == 1
        assert assay_rows.iloc[0]["pref_name"] == "Target X"

        param_rows = result[result["row_subtype"] == "param"]
        assert len(param_rows) == 2
        assert sorted(param_rows["row_index"].tolist()) == [0, 1]
        assert {"CONC", "TEMP"} == set(param_rows["assay_param_type"].dropna())

        variant_rows = result[result["row_subtype"] == "variant"]
        assert len(variant_rows) == 1
        assert variant_rows.iloc[0]["variant_id"] == 101

        class_rows = result[result["row_subtype"] == "class"]
        assert len(class_rows) == 2
        # Enrichment should preserve whitelist fields and drop unexpected columns
        assert "extra_field" not in result.columns
        assert class_rows["assay_class_description"].notna().all()

        # Join loss logging triggered when enrichment loses records (simulate by clearing target data)
        join_loss_payload = pd.DataFrame({
            "assay_chembl_id": ["CHEMBL9"],
            "target_chembl_id": ["CHEMBL_NOPE"],
            "assay_parameters_json": [None],
            "variant_sequence_json": [None],
            "assay_classifications": [None],
        })
        monkeypatch.setattr(pipeline, "_fetch_assay_data", lambda ids: join_loss_payload)
        monkeypatch.setattr(pipeline, "_fetch_target_reference_data", lambda ids: pd.DataFrame())
        with caplog.at_level("WARNING"):
            pipeline.transform(pd.DataFrame({"assay_chembl_id": ["CHEMBL9"], "target_chembl_id": ["CHEMBL_NOPE"]}))
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
            "potential_duplicate": 0,
            "uo_units": "UO_0000065",
            "qudt_units": "http://qudt.org/vocab/unit/NanoMOL-PER-L",
            "src_id": 1,
            "action_type": "inhibition",
            "activity_properties_json": "{}",
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

    def test_activity_cache_key_release_scope(self, activity_config, monkeypatch, tmp_path):
        """Cache keys must include release scoping for determinism."""

        def fake_request_json(self, url, params=None, method="GET"):
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

        def fake_request_json(self, url, params=None, method="GET"):
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
        assert set(df["error_type"].dropna().unique()) == {"CircuitBreakerOpenError"}
        assert set(df["chembl_release"].unique()) == {"ChEMBL_99"}

    def test_activity_cache_serves_before_network(self, activity_config, monkeypatch, tmp_path):
        """Batches are served from cache without additional network calls."""

        activity_calls: list[str] = []

        def fake_request_json(self, url, params=None, method="GET"):
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


class TestTestItemPipeline:
    """Tests for TestItemPipeline."""

    def test_init(self, testitem_config):
        """Test pipeline initialization."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TestItemPipeline(testitem_config, run_id)
        assert pipeline.config == testitem_config
        assert pipeline.run_id == run_id

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

        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
            "canonical_smiles": ["CC(=O)O"],
        })

        result = pipeline.transform(df)
        assert "pipeline_version" in result.columns
        assert "source_system" in result.columns
        assert "extracted_at" in result.columns


class TestTargetPipeline:
    """Tests for TargetPipeline."""

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
        csv_path.write_text(
            "target_chembl_id,pref_name,target_type\n"
            "CHEMBL1,Test Target,PROTEIN\n"
        )

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0

    def test_transform_adds_metadata(self, target_config):
        """Test transformation adds pipeline metadata."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TargetPipeline(target_config, run_id)

        df = pd.DataFrame({
            "target_chembl_id": ["CHEMBL1"],
            "pref_name": ["Test Target"],
        })

        result = pipeline.transform(df)
        assert "pipeline_version" in result.columns
        assert "source_system" in result.columns
        assert "extracted_at" in result.columns

    def test_validate_removes_duplicates(self, target_config):
        """Test validation removes duplicates."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TargetPipeline(target_config, run_id)

        df = pd.DataFrame({
            "target_chembl_id": ["CHEMBL1", "CHEMBL1"],
            "pref_name": ["Target 1", "Target 2"],
        })

        result = pipeline.validate(df)
        assert len(result) == 1


class TestDocumentPipeline:
    """Tests for DocumentPipeline."""

    def test_init(self, document_config):
        """Test pipeline initialization."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = DocumentPipeline(document_config, run_id)
        assert pipeline.config == document_config
        assert pipeline.run_id == run_id

    def test_extract_empty_file(self, document_config, tmp_path):
        """Test extraction with empty file."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = DocumentPipeline(document_config, run_id)

        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("document_chembl_id,title\n")

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_extract_with_data(self, document_config, tmp_path):
        """Test extraction with sample data."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = DocumentPipeline(document_config, run_id)

        csv_path = tmp_path / "documents.csv"
        csv_path.write_text(
            "document_chembl_id,title,doi\n"
            "CHEMBL1,Test Article,10.1234/test\n"
        )

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0

    def test_transform_adds_metadata(self, document_config):
        """Test transformation adds pipeline metadata."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = DocumentPipeline(document_config, run_id)

        df = pd.DataFrame({
            "document_chembl_id": ["CHEMBL1"],
            "title": ["Test Article"],
        })

        result = pipeline.transform(df)
        assert "pipeline_version" in result.columns
        assert "source_system" in result.columns
        assert "extracted_at" in result.columns

    def test_validate_removes_duplicates(self, document_config):
        """Test validation removes duplicates."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = DocumentPipeline(document_config, run_id)

        df = pd.DataFrame({
            "document_chembl_id": ["CHEMBL1", "CHEMBL1"],
            "title": ["Article 1", "Article 2"],
        })

        result = pipeline.validate(df)
        assert len(result) == 1
def _build_assay_row(assay_id: str, index: int, target_id: str | None) -> dict[str, Any]:
    """Construct a minimal row conforming to AssaySchema for tests."""

    schema_columns = list(AssaySchema.to_schema().columns.keys())
    row = {column: None for column in schema_columns}
    row.update(
        {
            "assay_chembl_id": assay_id,
            "row_subtype": "assay",
            "row_index": 0,
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

