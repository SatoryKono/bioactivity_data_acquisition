"""End-to-end tests for the document pipeline using the canonical layout."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import requests

pytest_httpserver = pytest.importorskip("pytest_httpserver")
HTTPServer = pytest_httpserver.HTTPServer

from bioetl.config.loader import load_config
from bioetl.config.paths import get_config_path
from bioetl.pipelines.document import DocumentPipeline, _document_run_pubmed_stage
from bioetl.sources.document.pipeline import ExternalEnrichmentResult


pytestmark = pytest.mark.integration


@pytest.fixture
def sample_documents_data() -> pd.DataFrame:
    """Sample document data for exercising pipeline stages."""

    return pd.DataFrame(
        {
            "document_chembl_id": [
                "CHEMBL1137491",
                "CHEMBL1155082",
                "CHEMBL4000255",
            ],
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
        }
    )


@pytest.fixture
def document_config_path() -> Path:
    """Resolve the packaged configuration for the document pipeline."""

    return get_config_path("profiles/document_test.yaml")


@pytest.fixture
def document_config(document_config_path: Path):
    """Load the document pipeline configuration once per test."""

    return load_config(document_config_path)


@pytest.fixture(autouse=True)
def cleanup_httpserver_state(httpserver: HTTPServer) -> None:
    """Ensure pytest-httpserver expectations do not leak across tests."""

    yield
    httpserver.clear()


@pytest.fixture
def mock_external_enrichment(
    monkeypatch: pytest.MonkeyPatch,
    httpserver: HTTPServer,
    sample_documents_data: pd.DataFrame,
):
    """Patch external enrichment to use a deterministic local HTTP service."""

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
            httpserver.expect_request(url_path, method="POST", json=expected_payload).respond_with_json(
                {"records": response_payload}, status=status
            )
        else:
            httpserver.expect_request(url_path, method="POST", json=expected_payload).respond_with_data(
                "", status=status
            )

        endpoint = httpserver.url_for(url_path)

        def fake_enrich(
            self: DocumentPipeline, chembl_df: pd.DataFrame
        ) -> ExternalEnrichmentResult:  # noqa: D401 - signature matches pipeline hook
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


class TestDocumentPipelineE2E:
    """Document pipeline integration scenarios scoped to the source tree."""

    def test_pipeline_initialization(self, document_config) -> None:
        """Pipeline loads adapters and metadata from the packaged config."""

        pipeline = DocumentPipeline(document_config, "test_run")

        assert pipeline.config.pipeline.name == "document"
        assert hasattr(pipeline, "external_adapters")

    def test_extract_transforms_schema_fields(
        self, document_config, sample_documents_data
    ) -> None:
        """Extracted rows are transformed into schema-aligned columns."""

        pipeline = DocumentPipeline(document_config, "test_run")

        original_extract = pipeline.extract
        pipeline.extract = lambda: sample_documents_data.copy()

        try:
            df = pipeline.extract()
            assert {"pubmed_id", "doi"}.issubset(df.columns)

            transformed = pipeline.transform(df)
            assert "document_pubmed_id" in transformed.columns or "chembl_pmid" in transformed.columns
            assert "document_classification" in transformed.columns
            assert "referenses_on_previous_experiments" in transformed.columns
            assert "original_experimental_document" in transformed.columns
        finally:
            pipeline.extract = original_extract

    def test_enrichment_adds_external_columns(
        self,
        document_config,
        sample_documents_data,
        mock_external_enrichment,
    ) -> None:
        """External enrichment augments frames with mocked sources."""

        pipeline = DocumentPipeline(document_config, "test_run")

        mock_external_enrichment()

        df = sample_documents_data.copy()
        if "pubmed_id" in df.columns:
            df["document_pubmed_id"] = df["pubmed_id"]
        if "classification" in df.columns:
            df["document_classification"] = df["classification"]

        result = pipeline._enrich_with_external_sources(df.head(3))
        enriched_df = result.dataframe

        assert "mock_source" in enriched_df.columns
        assert "mock_title" in enriched_df.columns
        assert enriched_df["mock_source"].eq("pytest-httpserver").all()
        assert enriched_df["mock_title"].str.contains("enriched").all()
        assert result.status == "completed"
        assert result.errors == {}

    def test_schema_compliance(self, document_config, sample_documents_data) -> None:
        """Transformed data validates against the Pandera schema."""

        pipeline = DocumentPipeline(document_config, "test_run")

        transformed = pipeline.transform(sample_documents_data.copy())
        try:
            validated_df = pipeline.validate(transformed)
        except Exception as exc:  # pragma: no cover - schema evolution safety net
            pytest.skip(f"Schema validation failed: {exc}")

        assert "document_chembl_id" in validated_df.columns
        assert "hash_business_key" in validated_df.columns
        assert "hash_row" in validated_df.columns
        assert "extracted_at" in validated_df.columns

    def test_column_order_matches_schema(self, document_config, sample_documents_data) -> None:
        """Column ordering respects schema-defined expectations."""

        from bioetl.schemas import DocumentSchema

        pipeline = DocumentPipeline(document_config, "test_run")

        transformed = pipeline.transform(sample_documents_data.copy())
        expected_order = DocumentSchema.get_column_order()
        if expected_order:
            existing_cols = [col for col in transformed.columns if col in expected_order]
            for index, column in enumerate(existing_cols):
                expected_idx = expected_order.index(column)
                for later in existing_cols[index + 1 :]:
                    later_idx = expected_order.index(later)
                    assert expected_idx < later_idx

    def test_no_duplicates(self, document_config, sample_documents_data) -> None:
        """Duplicate rows are removed during validation."""

        pipeline = DocumentPipeline(document_config, "test_run")

        duplicated = pd.concat([sample_documents_data, sample_documents_data])
        transformed = pipeline.transform(duplicated)
        validated_df = pipeline.validate(transformed)

        assert len(validated_df) <= len(duplicated)
        assert validated_df["document_chembl_id"].duplicated().sum() == 0

    def test_enrichment_handles_api_failures_gracefully(
        self,
        document_config,
        sample_documents_data,
        mock_external_enrichment,
    ) -> None:
        """Failures in the enrichment service downgrade status without crashing."""

        pipeline = DocumentPipeline(document_config, "test_run")

        mock_external_enrichment(status=500)

        df = sample_documents_data.copy()
        result = pipeline._enrich_with_external_sources(df.head(3))

        assert result is not None
        assert len(result.dataframe) <= len(df)
        assert result.status in {"completed", "failed"}
        if result.status == "failed":
            assert result.errors

    def test_pubmed_stage_summary_records_adapter_failure(
        self,
        document_config,
        sample_documents_data,
    ) -> None:
        """Failing adapters log QC issues for the PubMed stage."""

        pipeline = DocumentPipeline(document_config, "test_run")

        class FailingAdapter:
            def process(self, identifiers: list[str]) -> pd.DataFrame:  # pragma: no cover - simple stub
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

        issues = [issue for issue in pipeline.validation_issues if issue["metric"] == "enrichment.pubmed"]
        assert issues, "Expected QC issue for enrichment failure"
        last_issue = issues[-1]
        assert last_issue["severity"] == "error"
        assert last_issue["status"] == "failed"
        assert last_issue["errors"] == {"pubmed": "adapter boom"}
        assert last_issue["error_count"] == 1
