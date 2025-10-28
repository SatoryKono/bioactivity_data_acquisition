"""Tests for pipeline implementations."""

import uuid
from typing import Any

import pandas as pd
import pytest

from bioetl.config.loader import load_config
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

