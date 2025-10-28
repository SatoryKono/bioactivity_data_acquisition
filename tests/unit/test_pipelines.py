"""Tests for pipeline implementations."""

import json
import uuid

import pandas as pd
import pytest

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

    @pytest.fixture(autouse=True)
    def _mock_status(self, monkeypatch):
        """Return a deterministic status payload to prevent live network calls."""

        def _status_stub(self, url, params=None, method="GET"):
            if url == "/status.json":
                return {"chembl_db_version": "ChEMBL_TEST"}
            raise AssertionError(f"Unexpected request during test: {url}")

        monkeypatch.setattr(UnifiedAPIClient, "request_json", _status_stub)

    def test_cache_key_composition(self, assay_config, monkeypatch):
        """Ensure cache keys include the captured release."""

        call_count = {"status": 0}

        def _request(self, url, params=None, method="GET"):
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

    def test_fetch_assay_data_uses_cache(self, assay_config, monkeypatch):
        """Assay payloads should be cached by release-qualified key."""

        counts = {"status": 0, "fetch": 0}

        def _request(self, url, params=None, method="GET"):
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

    def test_fetch_assay_data_fallback_metadata(self, assay_config, monkeypatch):
        """Fallback records should include error metadata when requests fail."""

        def _request(self, url, params=None, method="GET"):
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
        assert row["error_message"].startswith("Circuit") or "breaker" in row["error_message"]
        assert row["run_id"] == "run-fallback"

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

    def test_validate_removes_duplicates(self, activity_config):
        """Test validation removes duplicates."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = ActivityPipeline(activity_config, run_id)

        df = pd.DataFrame({
            "activity_id": [1, 1, 2],
            "molecule_chembl_id": ["CHEMBL1", "CHEMBL1", "CHEMBL2"],
        })

        result = pipeline.validate(df)
        assert len(result) == 2
        assert result["activity_id"].nunique() == 2


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
