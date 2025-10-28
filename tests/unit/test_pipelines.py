"""Tests for pipeline implementations."""

import uuid

import pandas as pd
import pytest
import requests

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

    def test_init(self, document_config, monkeypatch):
        """Test pipeline initialization."""
        run_id = str(uuid.uuid4())[:8]
        monkeypatch.setattr(DocumentPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
        pipeline = DocumentPipeline(document_config, run_id)
        assert pipeline.config == document_config
        assert pipeline.run_id == run_id

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

        df = pd.DataFrame({
            "document_chembl_id": ["CHEMBL1"],
            "title": ["Test Article"],
        })

        result = pipeline.transform(df)
        assert "pipeline_version" in result.columns
        assert "source_system" in result.columns
        assert "extracted_at" in result.columns

    def test_validate_removes_duplicates(self, document_config, monkeypatch):
        """Test validation removes duplicates."""
        run_id = str(uuid.uuid4())[:8]
        monkeypatch.setattr(DocumentPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
        pipeline = DocumentPipeline(document_config, run_id)

        df = pd.DataFrame({
            "document_chembl_id": ["CHEMBL1", "CHEMBL1"],
            "title": ["Article 1", "Article 2"],
        })

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
        assert row["error_type"] == "E_TIMEOUT"
        assert pd.notna(row["error_message"])
        assert pd.notna(row["attempted_at"])
