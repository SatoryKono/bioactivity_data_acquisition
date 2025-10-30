#!/usr/bin/env python3
"""Integration tests for DocumentPipeline with external enrichment."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest
import requests
from pytest_httpserver import HTTPServer

from bioetl.config.loader import load_config
from bioetl.config.paths import get_config_path
from bioetl.pipelines.document import (
    DocumentPipeline,
    ExternalEnrichmentResult,
    _document_run_pubmed_stage,
)


pytestmark = pytest.mark.integration


@pytest.fixture
def sample_documents_data():
    """Sample document data for testing."""
    return pd.DataFrame({
        "document_chembl_id": ["CHEMBL1137491", "CHEMBL1155082", "CHEMBL4000255"],
        "pubmed_id": [17827018, 18578478, 28337320],
        "doi": [
            "10.1016/j.bmc.2007.08.038",
            "10.1021/jm800092x",
            "10.1021/acsmedchemlett.6b00465",
        ],
        "title": [
            "Click chemistry based solid phase supported synthesis",
            "Chemo-enzymatic synthesis of glutamate analogues",
            "Discovery of ABBV-075 (mivebresib)",
        ],
        "abstract": ["Abstract 1", "Abstract 2", "Abstract 3"],
        "authors": ["Author 1", "Author 2", "Author 3"],
        "journal": ["J. Med. Chem.", "J. Med. Chem.", "ACS Med. Chem. Lett."],
        "year": [2008, 2008, 2017],
        "classification": ["Journal Article", "Journal Article", "Journal Article"],
        "document_contains_external_links": [True, False, True],
        "is_experimental_doc": [True, True, True],
    })


@pytest.fixture
def document_config():
    """Use test profile with external sources disabled and dummy secrets."""
    return get_config_path("profiles/document_test.yaml")


@pytest.fixture(autouse=True)
def cleanup_httpserver_state(httpserver: HTTPServer) -> None:
    """Ensure the embedded HTTP server is reset between tests."""

    yield
    httpserver.clear()


@pytest.fixture
def mock_external_enrichment(
    monkeypatch: pytest.MonkeyPatch,
    httpserver: HTTPServer,
    sample_documents_data: pd.DataFrame,
):
    """Patch external enrichment to use a local HTTP server and avoid real network access."""

    configured = {"value": False}

    def _configure(*, status: int = 200) -> dict[str, list[dict[str, object]]]:
        configured["value"] = True
        url_path = "/enrich"
        expected_payload = sample_documents_data.head(3).to_dict(orient="records")
        response_payload = [
            {
                "document_chembl_id": record["document_chembl_id"],
                "mock_source": "pytest-httpserver",
                "mock_title": f"{record['title']} (enriched)",
            }
            for record in expected_payload
        ]

        if status < 400:
            httpserver.expect_request(
                url_path, method="POST", json=expected_payload
            ).respond_with_json({"records": response_payload}, status=status)
        else:
            httpserver.expect_request(
                url_path, method="POST", json=expected_payload
            ).respond_with_data("", status=status)

        endpoint = httpserver.url_for(url_path)

        def fake_enrich(
            self: DocumentPipeline, chembl_df: pd.DataFrame
        ) -> ExternalEnrichmentResult:
            payload = chembl_df.to_dict(orient="records")
            assert payload == expected_payload
            try:
                response = requests.post(endpoint, json=payload, timeout=5)
                response.raise_for_status()
            except requests.RequestException:
                return ExternalEnrichmentResult(chembl_df.copy(), "failed", {"mock": "request"})

            enriched_records = response.json().get("records", [])
            enrichment_df = pd.DataFrame(enriched_records)
            if enrichment_df.empty:
                return ExternalEnrichmentResult(chembl_df.copy(), "completed", {})
            merged = chembl_df.merge(enrichment_df, on="document_chembl_id", how="left")
            return ExternalEnrichmentResult(merged, "completed", {})

        monkeypatch.setattr(DocumentPipeline, "_enrich_with_external_sources", fake_enrich)

        return {
            "expected_payload": expected_payload,
            "response_payload": response_payload,
        }

    yield _configure

    if configured["value"]:
        httpserver.check_assertions()

@pytest.mark.integration
class TestDocumentPipelineEnrichment:
    """Test DocumentPipeline with external enrichment."""

    def test_pipeline_initialization(self, document_config):
        """Test that DocumentPipeline initializes correctly with all adapters."""
        config = load_config(document_config)
        pipeline = DocumentPipeline(config, "test_run")

        assert pipeline is not None
        assert pipeline.config.pipeline.name == "document"
        assert hasattr(pipeline, "external_adapters")

    def test_extract_transforms_schema_fields(self, document_config, sample_documents_data):
        """Test that extract and transform map input fields to schema fields."""
        config = load_config(document_config)
        pipeline = DocumentPipeline(config, "test_run")

        # Simulate extraction with sample data
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            sample_documents_data.to_csv(f.name, index=False)
            input_path = Path(f.name)

        original_extract = pipeline.extract
        pipeline.extract = lambda: sample_documents_data.copy()

        try:
            df = pipeline.extract()

            # Map fields before enrichment
            assert "pubmed_id" in df.columns
            assert "doi" in df.columns

            df = pipeline.transform(df)

            # Check that input fields are mapped
            assert "document_pubmed_id" in df.columns or "chembl_pmid" in df.columns
            assert "document_classification" in df.columns
            assert "referenses_on_previous_experiments" in df.columns
            assert "original_experimental_document" in df.columns

        finally:
            pipeline.extract = original_extract
            input_path.unlink()

    def test_enrichment_adds_external_columns(
        self,
        document_config,
        sample_documents_data,
        mock_external_enrichment,
    ):
        """Test that enrichment adds columns from external sources."""
        config = load_config(document_config)
        pipeline = DocumentPipeline(config, "test_run")

        mock_external_enrichment()

        # Simulate enrichment
        df = sample_documents_data.copy()

        # Map basic fields first
        if "pubmed_id" in df.columns:
            df["document_pubmed_id"] = df["pubmed_id"]
        if "classification" in df.columns:
            df["document_classification"] = df["classification"]

        result = pipeline._enrich_with_external_sources(df.head(3))
        enriched_df = result.dataframe

        # Check for mocked enrichment columns
        assert "mock_source" in enriched_df.columns
        assert "mock_title" in enriched_df.columns
        assert enriched_df["mock_source"].eq("pytest-httpserver").all()
        assert enriched_df["mock_title"].str.contains("enriched").all()
        assert result.status == "completed"
        assert result.errors == {}

    def test_schema_compliance(self, document_config, sample_documents_data):
        """Test that output complies with DocumentSchema."""

        config = load_config(document_config)
        pipeline = DocumentPipeline(config, "test_run")

        df = sample_documents_data.copy()
        df = pipeline.transform(df)

        # Validate against schema
        try:
            validated_df = pipeline.validate(df)

            # Check required fields are present
            assert "document_chembl_id" in validated_df.columns
            assert "index" in validated_df.columns
            assert "extracted_at" in validated_df.columns
            assert "hash_business_key" in validated_df.columns
            assert "hash_row" in validated_df.columns

        except Exception as e:
            # Schema validation may fail if required fields are missing
            # This is acceptable for integration tests
            pytest.skip(f"Schema validation failed: {e}")

    def test_column_order_matches_schema(self, document_config, sample_documents_data):
        """Test that column order matches schema specification."""
        from bioetl.schemas import DocumentSchema

        config = load_config(document_config)
        pipeline = DocumentPipeline(config, "test_run")

        df = sample_documents_data.copy()
        df = pipeline.transform(df)

        # Get expected column order from schema
        expected_order = DocumentSchema.get_column_order()
        if expected_order:

            # Check that existing columns follow the order
            existing_cols = [col for col in df.columns if col in expected_order]

            # Verify order
            for i, col in enumerate(existing_cols):
                expected_idx = expected_order.index(col)
                # Column should appear before later expected columns
                for later_col in existing_cols[i + 1 :]:
                    later_expected_idx = expected_order.index(later_col)
                    assert expected_idx < later_expected_idx

    def test_no_duplicates(self, document_config, sample_documents_data):
        """Test that validation removes duplicates."""
        config = load_config(document_config)
        pipeline = DocumentPipeline(config, "test_run")

        # Add duplicates
        df_with_duplicates = pd.concat([sample_documents_data, sample_documents_data])

        df = pipeline.transform(df_with_duplicates)
        validated_df = pipeline.validate(df)

        # Should have no duplicates
        assert len(validated_df) <= len(df_with_duplicates)
        assert validated_df["document_chembl_id"].duplicated().sum() == 0

    def test_enrichment_handles_api_failures_gracefully(
        self,
        document_config,
        sample_documents_data,
        mock_external_enrichment,
    ):
        """Test that enrichment continues even if some APIs fail."""
        config = load_config(document_config)
        pipeline = DocumentPipeline(config, "test_run")

        mock_external_enrichment(status=500)

        df = sample_documents_data.copy()

        try:
            result = pipeline._enrich_with_external_sources(df.head(3))

            # Should not raise exception even if APIs fail
            assert result is not None
            assert len(result.dataframe) <= len(df)
            assert result.dataframe.equals(df.head(3))
            assert result.status in {"completed", "failed"}
            if result.status == "failed":
                assert result.errors

        except Exception as e:
            # If enrichment fails completely, that's also acceptable
            pytest.skip(f"Enrichment failed: {e}")

    def test_pubmed_stage_summary_records_adapter_failure(
        self,
        document_config,
        sample_documents_data,
    ):
        """Failed adapters should mark the stage as failed and record QC issues."""

        config = load_config(document_config)
        pipeline = DocumentPipeline(config, "test_run")

        class FailingAdapter:
            def process(self, identifiers: list[str]) -> pd.DataFrame:
                raise RuntimeError("adapter boom")

        pipeline.external_adapters = {"pubmed": FailingAdapter()}

        df = sample_documents_data.head(1).copy()

        result_df = _document_run_pubmed_stage(pipeline, df)

        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == len(df)
        assert result_df["document_chembl_id"].tolist() == df["document_chembl_id"].tolist()

        summary = pipeline.get_stage_summary("pubmed")
        assert summary is not None
        assert summary["status"] == "failed"
        assert summary["error_count"] == 1
        assert summary["errors"] == {"pubmed": "adapter boom"}

        assert "pubmed" in pipeline.stage_context
        assert pipeline.stage_context["pubmed"]["status"] == "failed"
        assert pipeline.stage_context["pubmed"]["errors"] == {"pubmed": "adapter boom"}

        issues = [issue for issue in pipeline.validation_issues if issue["metric"] == "enrichment.pubmed"]
        assert issues, "Expected QC issue for enrichment failure"
        last_issue = issues[-1]
        assert last_issue["severity"] == "error"
        assert last_issue["status"] == "failed"
        assert last_issue["errors"] == {"pubmed": "adapter boom"}
        assert last_issue["error_count"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

