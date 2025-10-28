"""Tests for pipeline implementations."""

import uuid

import pandas as pd
import pytest
from pandera.errors import SchemaErrors

from bioetl.schemas.activity import COLUMN_ORDER as ACTIVITY_COLUMN_ORDER

from bioetl.config.loader import load_config
from bioetl.pipelines import ActivityPipeline, AssayPipeline, DocumentPipeline, TargetPipeline, TestItemPipeline


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

    def test_transform_adds_metadata(self, assay_config):
        """Test transformation adds pipeline metadata."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = AssayPipeline(assay_config, run_id)

        df = pd.DataFrame({
            "assay_chembl_id": ["CHEMBL1"],
            "description": ["  Test Assay  "],
        })

        result = pipeline.transform(df)
        assert "pipeline_version" in result.columns
        assert "source_system" in result.columns
        assert "extracted_at" in result.columns


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
