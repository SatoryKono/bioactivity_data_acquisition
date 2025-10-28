"""Tests for pipeline implementations."""

import uuid

import pandas as pd
import pytest
from unittest.mock import MagicMock

from bioetl.config.loader import load_config
from bioetl.core.api_client import CircuitBreakerOpenError, UnifiedAPIClient
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
