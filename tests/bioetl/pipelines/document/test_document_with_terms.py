"""Unit tests for document enrichment with document_term."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.clients.client_chembl import ChemblClient
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.pipelines.chembl.document.normalize import (
    _escape_pipe,  # type: ignore[reportPrivateUsage]
    aggregate_terms,
    enrich_with_document_terms,
)


@pytest.fixture
def mock_chembl_client() -> ChemblClient:
    """Create a mock ChemblClient for testing."""
    mock_api_client = MagicMock(spec=UnifiedAPIClient)
    return ChemblClient(mock_api_client)


@pytest.fixture
def sample_document_df() -> pd.DataFrame:
    """Sample document DataFrame for enrichment testing."""
    return pd.DataFrame(
        {
            "document_chembl_id": ["CHEMBL1000", "CHEMBL1001", "CHEMBL1002"],
            "title": ["Title 1", "Title 2", "Title 3"],
            "doi": ["10.1000/test1", "10.1000/test2", None],
        }
    )


@pytest.fixture
def document_term_enrichment_config() -> dict[str, Any]:
    """Configuration for document_term enrichment."""
    return {
        "enabled": True,
        "select_fields": ["document_chembl_id", "term", "weight"],
        "page_limit": 1000,
        "sort": "weight_desc",
    }


class TestEscapePipe:
    """Test suite for _escape_pipe function."""

    def test_escape_pipe_basic(self) -> None:
        """Test basic pipe escaping."""
        assert _escape_pipe("A|B") == "A\\|B"

    def test_escape_pipe_backslash(self) -> None:
        """Test backslash escaping."""
        assert _escape_pipe("A\\B") == "A\\\\B"

    def test_escape_pipe_both(self) -> None:
        """Test escaping both pipe and backslash."""
        assert _escape_pipe("A|B\\C") == "A\\|B\\\\C"

    def test_escape_pipe_no_special(self) -> None:
        """Test string without special characters."""
        assert _escape_pipe("ABC") == "ABC"

    def test_escape_pipe_empty(self) -> None:
        """Test empty string."""
        assert _escape_pipe("") == ""

    def test_escape_pipe_none(self) -> None:
        """Test None value."""
        assert _escape_pipe(None) == ""  # type: ignore[arg-type]


class TestAggregateTerms:
    """Test suite for aggregate_terms function."""

    def test_aggregate_terms_empty(self) -> None:
        """Test aggregation with empty list."""
        rows: list[dict[str, Any]] = []
        result = aggregate_terms(rows, sort="weight_desc")
        assert result == {}

    def test_aggregate_terms_single(self) -> None:
        """Test aggregation with single term."""
        rows = [
            {"document_chembl_id": "CHEMBL1000", "term": "ABC", "weight": 0.9},
        ]
        result = aggregate_terms(rows, sort="weight_desc")
        assert result["CHEMBL1000"]["term"] == "ABC"
        assert result["CHEMBL1000"]["weight"] == "0.9"

    def test_aggregate_terms_multiple(self) -> None:
        """Test aggregation with multiple terms, sorted by weight descending."""
        rows = [
            {"document_chembl_id": "CHEMBL1000", "term": "A", "weight": 0.9},
            {"document_chembl_id": "CHEMBL1000", "term": "B", "weight": 0.7},
            {"document_chembl_id": "CHEMBL1000", "term": "C", "weight": None},
        ]
        result = aggregate_terms(rows, sort="weight_desc")
        # Should be sorted by weight descending: A (0.9), B (0.7), C (None -> -inf)
        assert result["CHEMBL1000"]["term"] == "A|B|C"
        assert result["CHEMBL1000"]["weight"] == "0.9|0.7|"

    def test_aggregate_terms_multiple_documents(self) -> None:
        """Test aggregation with multiple documents."""
        rows = [
            {"document_chembl_id": "CHEMBL1000", "term": "A", "weight": 0.9},
            {"document_chembl_id": "CHEMBL1001", "term": "B", "weight": 0.8},
            {"document_chembl_id": "CHEMBL1000", "term": "C", "weight": 0.5},
        ]
        result = aggregate_terms(rows, sort="weight_desc")
        assert result["CHEMBL1000"]["term"] == "A|C"
        assert result["CHEMBL1000"]["weight"] == "0.9|0.5"
        assert result["CHEMBL1001"]["term"] == "B"
        assert result["CHEMBL1001"]["weight"] == "0.8"

    def test_aggregate_terms_escape_pipe(self) -> None:
        """Test escaping pipe in terms."""
        rows = [
            {"document_chembl_id": "CHEMBL1000", "term": "A|B", "weight": 0.9},
            {"document_chembl_id": "CHEMBL1000", "term": "C", "weight": 0.7},
        ]
        result = aggregate_terms(rows, sort="weight_desc")
        assert result["CHEMBL1000"]["term"] == "A\\|B|C"
        assert result["CHEMBL1000"]["weight"] == "0.9|0.7"

    def test_aggregate_terms_no_weight(self) -> None:
        """Test aggregation with missing weight."""
        rows = [
            {"document_chembl_id": "CHEMBL1000", "term": "A", "weight": None},
            {"document_chembl_id": "CHEMBL1000", "term": "B", "weight": ""},
        ]
        result = aggregate_terms(rows, sort="weight_desc")
        assert result["CHEMBL1000"]["term"] == "A|B"
        assert result["CHEMBL1000"]["weight"] == "|"

    def test_aggregate_terms_missing_document_id(self) -> None:
        """Test aggregation with missing document_chembl_id."""
        rows = [
            {"document_chembl_id": None, "term": "A", "weight": 0.9},
            {"document_chembl_id": "CHEMBL1000", "term": "B", "weight": 0.8},
        ]
        result = aggregate_terms(rows, sort="weight_desc")
        assert "CHEMBL1000" in result
        assert result["CHEMBL1000"]["term"] == "B"
        assert None not in result


class TestEnrichWithDocumentTerms:
    """Test suite for enrich_with_document_terms function."""

    def test_enrich_with_document_terms_no_terms(
        self,
        mock_chembl_client: ChemblClient,
        sample_document_df: pd.DataFrame,
        document_term_enrichment_config: dict[str, Any],
    ) -> None:
        """Test enrichment when document has no terms."""
        # Mock fetch_document_terms_by_ids to return empty dict
        mock_chembl_client.fetch_document_terms_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={}
        )

        result = enrich_with_document_terms(
            sample_document_df,
            mock_chembl_client,
            document_term_enrichment_config,
        )

        assert "term" in result.columns
        assert "weight" in result.columns
        assert all(result["term"] == "")
        assert all(result["weight"] == "")

    def test_enrich_with_document_terms_with_terms(
        self,
        mock_chembl_client: ChemblClient,
        sample_document_df: pd.DataFrame,
        document_term_enrichment_config: dict[str, Any],
    ) -> None:
        """Test enrichment when document has terms."""
        # Mock fetch_document_terms_by_ids to return sample data
        mock_chembl_client.fetch_document_terms_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL1000": [
                    {"document_chembl_id": "CHEMBL1000", "term": "ABC", "weight": 0.9},
                    {"document_chembl_id": "CHEMBL1000", "term": "DEF", "weight": 0.7},
                ],
                "CHEMBL1001": [
                    {"document_chembl_id": "CHEMBL1001", "term": "GHI", "weight": 0.8},
                ],
            }
        )

        result = enrich_with_document_terms(
            sample_document_df,
            mock_chembl_client,
            document_term_enrichment_config,
        )

        assert "term" in result.columns
        assert "weight" in result.columns

        # Check CHEMBL1000 (sorted by weight descending)
        row_1000 = result[result["document_chembl_id"] == "CHEMBL1000"].iloc[0]
        assert row_1000["term"] == "ABC|DEF"
        assert row_1000["weight"] == "0.9|0.7"

        # Check CHEMBL1001
        row_1001 = result[result["document_chembl_id"] == "CHEMBL1001"].iloc[0]
        assert row_1001["term"] == "GHI"
        assert row_1001["weight"] == "0.8"

        # Check CHEMBL1002 (no terms)
        row_1002 = result[result["document_chembl_id"] == "CHEMBL1002"].iloc[0]
        assert row_1002["term"] == ""
        assert row_1002["weight"] == ""

    def test_enrich_with_document_terms_preserves_order(
        self,
        mock_chembl_client: ChemblClient,
        sample_document_df: pd.DataFrame,
        document_term_enrichment_config: dict[str, Any],
    ) -> None:
        """Test that enrichment preserves row order."""
        # Mock fetch_document_terms_by_ids
        mock_chembl_client.fetch_document_terms_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={}
        )

        original_index = sample_document_df.index.copy()
        result = enrich_with_document_terms(
            sample_document_df,
            mock_chembl_client,
            document_term_enrichment_config,
        )

        assert list(result.index) == list(original_index)

    def test_enrich_with_document_terms_empty_dataframe(
        self,
        mock_chembl_client: ChemblClient,
        document_term_enrichment_config: dict[str, Any],
    ) -> None:
        """Test enrichment with empty DataFrame."""
        empty_df = pd.DataFrame({"document_chembl_id": []})

        result = enrich_with_document_terms(
            empty_df,
            mock_chembl_client,
            document_term_enrichment_config,
        )

        assert "term" in result.columns
        assert "weight" in result.columns
        assert len(result) == 0

    def test_enrich_with_document_terms_missing_column(
        self,
        mock_chembl_client: ChemblClient,
        document_term_enrichment_config: dict[str, Any],
    ) -> None:
        """Test enrichment when document_chembl_id column is missing."""
        df = pd.DataFrame({"title": ["Title 1", "Title 2"]})

        result = enrich_with_document_terms(
            df,
            mock_chembl_client,
            document_term_enrichment_config,
        )

        assert "term" in result.columns
        assert "weight" in result.columns
        assert all(result["term"] == "")
        assert all(result["weight"] == "")
