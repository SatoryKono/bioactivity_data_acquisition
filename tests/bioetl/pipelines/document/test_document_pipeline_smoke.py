"""Integration smoke tests for document pipeline with enrichment."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from bioetl.config import load_config
from bioetl.pipelines.document.document import ChemblDocumentPipeline


def create_mock_document_data(count: int = 3) -> list[dict[str, object]]:
    """Create mock document data for testing."""
    documents: list[dict[str, object]] = []
    for i in range(count):
        doc: dict[str, object] = {
            "document_chembl_id": f"CHEMBL{1000 + i}",
            "doc_type": "Publication",
            "journal": "Journal of Test",
            "journal_full_title": "Journal of Test - Full Title",
            "doi": f"10.1000/test{i + 1}",
            "src_id": str(i + 1),
            "title": f"Test Document {i + 1}",
            "abstract": f"Abstract for document {i + 1}",
            "year": 2023 + i,
            "journal_abbrev": "J. Test",
            "volume": str(i + 1),
            "issue": str(i + 1),
            "first_page": str((i + 1) * 10),
            "last_page": str((i + 1) * 10 + 5),
            "pubmed_id": 1000000 + i,
            "authors": f"Author {i + 1}, Author {i + 2}",
        }
        documents.append(doc)
    return documents


def create_mock_document_term_data() -> dict[str, list[dict[str, object]]]:
    """Create mock document_term data for testing."""
    return {
        "CHEMBL1000": [
            {"document_chembl_id": "CHEMBL1000", "term": "term1", "weight": 0.9},
            {"document_chembl_id": "CHEMBL1000", "term": "term2", "weight": 0.7},
        ],
        "CHEMBL1001": [
            {"document_chembl_id": "CHEMBL1001", "term": "term3", "weight": 0.8},
        ],
        # CHEMBL1002 has no terms
    }


def setup_mock_api_client(
    mock_documents: list[dict[str, object]],
    mock_document_terms: dict[str, list[dict[str, object]]] | None = None,
) -> MagicMock:
    """Setup mock API client and factory for testing."""
    mock_client = MagicMock()

    # Create status response
    mock_status_response = MagicMock()
    mock_status_response.json.return_value = {"chembl_db_version": "33", "api_version": "1.0"}
    mock_status_response.status_code = 200
    mock_status_response.headers = {}

    # Create document response
    mock_document_response = MagicMock()
    mock_document_response.json.return_value = {
        "page_meta": {"offset": 0, "limit": 25, "count": len(mock_documents), "next": None},
        "documents": mock_documents,
    }
    mock_document_response.status_code = 200
    mock_document_response.headers = {}

    # Create document_term response if needed
    mock_document_term_response = MagicMock()
    if mock_document_terms:
        all_terms: list[dict[str, object]] = []
        for terms in mock_document_terms.values():
            all_terms.extend(terms)
        mock_document_term_response.json.return_value = {
            "page_meta": {"offset": 0, "limit": 25, "count": len(all_terms), "next": None},
            "document_terms": all_terms,
        }
    else:
        mock_document_term_response.json.return_value = {
            "page_meta": {"offset": 0, "limit": 25, "count": 0, "next": None},
            "document_terms": [],
        }
    mock_document_term_response.status_code = 200
    mock_document_term_response.headers = {}

    # Use a function to handle multiple calls
    call_count = {"count": 0}
    responses = [mock_status_response]

    def get_side_effect(url: str, *args: object, **kwargs: object) -> MagicMock:
        call_count["count"] += 1
        if "/status" in url or "/status.json" in url:
            if call_count["count"] <= len(responses):
                return responses[call_count["count"] - 1]
            return mock_status_response
        elif "/document.json" in url:
            return mock_document_response
        elif "/document_term.json" in url:
            return mock_document_term_response
        return mock_status_response

    mock_client.get.side_effect = get_side_effect
    return mock_client


@pytest.mark.integration
class TestDocumentPipelineSmoke:
    """Integration smoke tests for document pipeline."""

    def test_document_pipeline_without_enrichment(self, tmp_path: Path) -> None:
        """Test document pipeline without enrichment (enabled: false)."""
        config_path = Path(__file__).parent.parent.parent / "configs" / "pipelines" / "document" / "document_chembl.yaml"
        config = load_config(config_path)

        # Ensure enrichment is disabled
        if config.chembl and config.chembl.get("document"):
            if config.chembl["document"].get("enrich"):
                if config.chembl["document"]["enrich"].get("document_term"):
                    # Convert to dict to allow modification
                    chembl_dict = dict(config.chembl) if config.chembl else {}
                    document_dict = dict(chembl_dict.get("document", {}))
                    enrich_dict = dict(document_dict.get("enrich", {}))
                    document_term_dict = dict(enrich_dict.get("document_term", {}))
                    document_term_dict["enabled"] = False
                    enrich_dict["document_term"] = document_term_dict
                    document_dict["enrich"] = enrich_dict
                    chembl_dict["document"] = document_dict
                    config.chembl = chembl_dict

        mock_documents = create_mock_document_data(count=3)

        with patch("bioetl.core.client_factory.APIClientFactory.for_source") as mock_factory:
            mock_client = setup_mock_api_client(mock_documents, mock_document_terms=None)
            mock_factory.return_value = mock_client

            pipeline = ChemblDocumentPipeline(config, run_id="test-run-001")
            result = pipeline.run(tmp_path)

            # Check that files were created
            assert result.write_result.dataset.exists()

            # Read the dataset to verify content
            if result.write_result.dataset.suffix == ".parquet":
                df: pd.DataFrame = pd.read_parquet(result.write_result.dataset)  # type: ignore[assignment]
            else:
                df: pd.DataFrame = pd.read_csv(result.write_result.dataset)  # type: ignore[assignment]
            assert len(df) == 3
            assert "document_chembl_id" in df.columns
            assert "doc_type" in df.columns
            assert "journal_full_title" in df.columns
            assert "src_id" in df.columns

            # Check that term and weight columns exist (but empty when enrichment disabled)
            assert "term" in df.columns
            assert "weight" in df.columns

    def test_document_pipeline_with_enrichment(self, tmp_path: Path) -> None:
        """Test document pipeline with enrichment enabled."""
        config_path = Path(__file__).parent.parent.parent / "configs" / "pipelines" / "document" / "document_chembl.yaml"
        config = load_config(config_path)

        # Enable enrichment - convert to dict to allow modification
        chembl_dict = dict(config.chembl) if config.chembl else {}
        if "document" not in chembl_dict:
            chembl_dict["document"] = {}
        if "enrich" not in chembl_dict["document"]:
            chembl_dict["document"]["enrich"] = {}
        if "document_term" not in chembl_dict["document"]["enrich"]:
            chembl_dict["document"]["enrich"]["document_term"] = {}
        chembl_dict["document"]["enrich"]["document_term"]["enabled"] = True
        chembl_dict["document"]["enrich"]["document_term"]["select_fields"] = [
            "document_chembl_id",
            "term",
            "weight",
        ]
        chembl_dict["document"]["enrich"]["document_term"]["page_limit"] = 1000
        chembl_dict["document"]["enrich"]["document_term"]["sort"] = "weight_desc"
        config.chembl = chembl_dict

        mock_documents = create_mock_document_data(count=3)
        mock_document_terms = create_mock_document_term_data()

        with patch("bioetl.core.client_factory.APIClientFactory.for_source") as mock_factory:
            mock_client = setup_mock_api_client(mock_documents, mock_document_terms=mock_document_terms)
            mock_factory.return_value = mock_client

            pipeline = ChemblDocumentPipeline(config, run_id="test-run-002")
            result = pipeline.run(tmp_path)

            # Check that files were created
            assert result.write_result.dataset.exists()

            # Read the dataset to verify content
            if result.write_result.dataset.suffix == ".parquet":
                df: pd.DataFrame = pd.read_parquet(result.write_result.dataset)  # type: ignore[assignment]
            else:
                df: pd.DataFrame = pd.read_csv(result.write_result.dataset)  # type: ignore[assignment]
            assert len(df) == 3

            # Check that term and weight columns are populated
            assert "term" in df.columns
            assert "weight" in df.columns

            # Check CHEMBL1000 has terms
            row_1000 = df[df["document_chembl_id"] == "CHEMBL1000"].iloc[0]
            assert row_1000["term"] == "term1|term2"  # Sorted by weight descending
            assert row_1000["weight"] == "0.9|0.7"

            # Check CHEMBL1001 has terms
            row_1001 = df[df["document_chembl_id"] == "CHEMBL1001"].iloc[0]
            assert row_1001["term"] == "term3"
            assert row_1001["weight"] == "0.8"

            # Check CHEMBL1002 has no terms (empty strings or nan)
            # After deduplication, CHEMBL1002 might not be present or might have nan values
            rows_1002 = df[df["document_chembl_id"] == "CHEMBL1002"]
            if len(rows_1002) > 0:
                row_1002 = rows_1002.iloc[0]
                # term and weight should be empty string or nan for documents without terms
                term_value = row_1002["term"]
                weight_value = row_1002["weight"]
                assert term_value == "" or pd.isna(term_value), f"Expected empty string or nan, got {term_value}"
                assert weight_value == "" or pd.isna(weight_value), f"Expected empty string or nan, got {weight_value}"

    def test_document_pipeline_columns_order(self, tmp_path: Path) -> None:
        """Test that document pipeline maintains correct column order."""
        config_path = Path(__file__).parent.parent.parent / "configs" / "pipelines" / "document" / "document_chembl.yaml"
        config = load_config(config_path)

        mock_documents = create_mock_document_data(count=2)

        with patch("bioetl.core.client_factory.APIClientFactory.for_source") as mock_factory:
            mock_client = setup_mock_api_client(mock_documents, mock_document_terms=None)
            mock_factory.return_value = mock_client

            pipeline = ChemblDocumentPipeline(config, run_id="test-run-003")
            result = pipeline.run(tmp_path)

            # Check that files were created
            assert result.write_result.dataset.exists()

            # Read the dataset to verify content
            if result.write_result.dataset.suffix == ".parquet":
                df: pd.DataFrame = pd.read_parquet(result.write_result.dataset)  # type: ignore[assignment]
            else:
                df: pd.DataFrame = pd.read_csv(result.write_result.dataset)  # type: ignore[assignment]

            # Check that term and weight are at the end
            columns = list(df.columns)
            assert "term" in columns
            assert "weight" in columns
            # term and weight should be near the end (after hash_row)
            term_index = columns.index("term")
            weight_index = columns.index("weight")
            assert term_index > columns.index("hash_row")
            assert weight_index > term_index

