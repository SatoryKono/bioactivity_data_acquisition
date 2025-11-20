"""Integration smoke tests for document pipeline with enrichment."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from bioetl.clients.chembl_entity_factory import ChemblClientBundle
from bioetl.clients.client_chembl import ChemblClient
from bioetl.config import load_config
from bioetl.pipelines.chembl.document import run as document_run
from bioetl.pipelines.chembl.helpers import build_dataframe

EXPECTED_SELECT_FIELDS = [
    "document_chembl_id",
    "doc_type",
    "journal",
    "journal_full_title",
    "doi",
    "src_id",
    "title",
    "abstract",
    "year",
    "volume",
    "issue",
    "first_page",
    "last_page",
    "pubmed_id",
    "authors",
]


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
        if "/status" in url:
            if call_count["count"] <= len(responses):
                return responses[call_count["count"] - 1]
            return mock_status_response
        if "/document.json" in url:
            return mock_document_response
        if "/document_term.json" in url:
            return mock_document_term_response
        return mock_status_response

    mock_client.get.side_effect = get_side_effect
    return mock_client


def make_document_client_mock(
    mock_documents: list[dict[str, object]],
) -> MagicMock:
    """Create a mock ChemblDocumentClient with predictable iterators."""

    document_client = MagicMock()
    document_client.batch_size = 20

    def iterate_all_side_effect(
        *,
        limit: int | None = None,
        page_size: int | None = None,
        select_fields: list[str] | None = None,
    ):
        return (dict(doc) for doc in mock_documents)

    def iterate_by_ids_side_effect(
        ids: list[str] | tuple[str, ...],
        *,
        select_fields: list[str] | None = None,
    ):
        id_set = {str(value) for value in ids}
        filtered = [doc for doc in mock_documents if doc.get("document_chembl_id") in id_set]
        selected = filtered if filtered else mock_documents
        return (dict(doc) for doc in selected)

    document_client.iterate_all.side_effect = iterate_all_side_effect
    document_client.iterate_by_ids.side_effect = iterate_by_ids_side_effect
    return document_client


@pytest.mark.integration
class TestDocumentPipelineSmoke:
    """Integration smoke tests for document pipeline."""

    def test_document_pipeline_without_enrichment(self, tmp_path: Path) -> None:
        """Test document pipeline without enrichment (enabled: false)."""
        config_path = (
            Path(__file__).parent.parent.parent
            / "configs"
            / "pipelines"
            / "document"
            / "document_chembl.yaml"
        )
        config = load_config(config_path)
        config.cli.input_file = None

        # Ensure enrichment is disabled
        document_cfg = config.chembl.get("document") if config.chembl else None
        enrich_cfg = document_cfg.get("enrich") if document_cfg else None
        document_term_cfg = enrich_cfg.get("document_term") if enrich_cfg else None
        if document_term_cfg:
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

        with (
            patch("bioetl.core.APIClientFactory.for_source") as mock_factory,
        ):
            mock_client = setup_mock_api_client(mock_documents, mock_document_terms=None)
            mock_factory.return_value = mock_client

            mock_doc_client = make_document_client_mock(mock_documents)
            
            # Create mock bundle with document client
            mock_bundle = MagicMock(spec=ChemblClientBundle)
            mock_bundle.chembl_client = MagicMock(spec=ChemblClient)
            mock_bundle.chembl_client.handshake.return_value = {"chembl_db_version": "33"}
            mock_bundle.api_client = mock_client
            mock_bundle.entity_client = mock_doc_client
            mock_bundle.entity_name = "document"
            mock_bundle.source_name = "chembl"
            mock_bundle.base_url = "https://www.ebi.ac.uk/chembl/api/data"
            mock_bundle.entity_config = None
            mock_bundle.source_config = None

            pipeline = document_run.ChemblDocumentPipeline(config, run_id="test-run-001")
            
            # Create DataFrame from mock documents
            extract_df = build_dataframe(mock_documents)
            
            with (
                patch.object(pipeline, "build_chembl_entity_bundle", return_value=mock_bundle),
                patch.object(pipeline, "extract_all", return_value=extract_df),
            ):
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
        config_path = (
            Path(__file__).parent.parent.parent
            / "configs"
            / "pipelines"
            / "document"
            / "document_chembl.yaml"
        )
        config = load_config(config_path)
        config.cli.input_file = None

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

        with (
            patch("bioetl.core.APIClientFactory.for_source") as mock_factory,
        ):
            mock_client = setup_mock_api_client(
                mock_documents, mock_document_terms=mock_document_terms
            )
            mock_factory.return_value = mock_client

            mock_doc_client = make_document_client_mock(mock_documents)
            
            # Create mock bundle with document client
            mock_bundle = MagicMock(spec=ChemblClientBundle)
            mock_bundle.chembl_client = MagicMock(spec=ChemblClient)
            mock_bundle.chembl_client.handshake.return_value = {"chembl_db_version": "33"}
            # Mock fetch_document_terms_by_ids to return the mock document terms
            mock_bundle.chembl_client.fetch_document_terms_by_ids = MagicMock(
                return_value=mock_document_terms
            )
            mock_bundle.api_client = mock_client
            mock_bundle.entity_client = mock_doc_client
            mock_bundle.entity_name = "document"
            mock_bundle.source_name = "chembl"
            mock_bundle.base_url = "https://www.ebi.ac.uk/chembl/api/data"
            mock_bundle.entity_config = None
            mock_bundle.source_config = None

            pipeline = document_run.ChemblDocumentPipeline(config, run_id="test-run-002")
            
            # Create DataFrame from mock documents
            extract_df = build_dataframe(mock_documents)
            
            with (
                patch.object(pipeline, "build_chembl_entity_bundle", return_value=mock_bundle),
                patch.object(pipeline, "extract_all", return_value=extract_df),
            ):
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
                assert term_value == "" or pd.isna(term_value), (
                    f"Expected empty string or nan, got {term_value}"
                )
                assert weight_value == "" or pd.isna(weight_value), (
                    f"Expected empty string or nan, got {weight_value}"
                )

    def test_document_pipeline_columns_order(self, tmp_path: Path) -> None:
        """Test that document pipeline maintains correct column order."""
        config_path = (
            Path(__file__).parent.parent.parent
            / "configs"
            / "pipelines"
            / "document"
            / "document_chembl.yaml"
        )
        config = load_config(config_path)
        config.cli.input_file = None

        mock_documents = create_mock_document_data(count=2)

        with (
            patch("bioetl.core.APIClientFactory.for_source") as mock_factory,
        ):
            mock_client = setup_mock_api_client(mock_documents, mock_document_terms=None)
            mock_factory.return_value = mock_client

            mock_doc_client = make_document_client_mock(mock_documents)
            
            # Create mock bundle with document client
            mock_bundle = MagicMock(spec=ChemblClientBundle)
            mock_bundle.chembl_client = MagicMock(spec=ChemblClient)
            mock_bundle.chembl_client.handshake.return_value = {"chembl_db_version": "33"}
            mock_bundle.api_client = mock_client
            mock_bundle.entity_client = mock_doc_client
            mock_bundle.entity_name = "document"
            mock_bundle.source_name = "chembl"
            mock_bundle.base_url = "https://www.ebi.ac.uk/chembl/api/data"
            mock_bundle.entity_config = None
            mock_bundle.source_config = None

            pipeline = document_run.ChemblDocumentPipeline(config, run_id="test-run-003")
            
            # Create DataFrame from mock documents
            extract_df = build_dataframe(mock_documents)
            
            with (
                patch.object(pipeline, "build_chembl_entity_bundle", return_value=mock_bundle),
                patch.object(pipeline, "extract_all", return_value=extract_df),
            ):
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
