"""Unit tests for ChemblAssayClient."""

from __future__ import annotations

from unittest.mock import MagicMock, Mock

import pytest

from bioetl.clients.chembl import ChemblClient
from bioetl.clients.assay.chembl_assay import ChemblAssayClient
from bioetl.core.api_client import UnifiedAPIClient


@pytest.fixture
def mock_unified_client() -> MagicMock:
    """Mock UnifiedAPIClient for testing."""
    return MagicMock(spec=UnifiedAPIClient)


@pytest.fixture
def mock_chembl_client(mock_unified_client: MagicMock) -> MagicMock:
    """Mock ChemblClient for testing."""
    mock_client = MagicMock(spec=ChemblClient)
    # Set up default return values for methods
    mock_client.handshake = MagicMock(return_value={})
    mock_client.paginate = MagicMock(return_value=iter([]))
    return mock_client


@pytest.mark.unit
class TestChemblAssayClient:
    """Test suite for ChemblAssayClient."""

    def test_init_valid(self, mock_chembl_client: MagicMock) -> None:
        """Test ChemblAssayClient initialization with valid parameters."""
        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        assert client._client == mock_chembl_client
        assert client._batch_size == 25
        assert client._max_url_length == 2000

    def test_init_batch_size_capped(self, mock_chembl_client: ChemblClient) -> None:
        """Test that batch_size is capped at 25."""
        client = ChemblAssayClient(
            mock_chembl_client,
            batch_size=100,
            max_url_length=2000,
        )

        assert client._batch_size == 25

    def test_init_invalid_batch_size(self, mock_chembl_client: ChemblClient) -> None:
        """Test initialization with invalid batch_size raises error."""
        with pytest.raises(ValueError, match="batch_size must be a positive integer"):
            ChemblAssayClient(mock_chembl_client, batch_size=0, max_url_length=2000)

    def test_init_invalid_max_url_length(self, mock_chembl_client: ChemblClient) -> None:
        """Test initialization with invalid max_url_length raises error."""
        with pytest.raises(ValueError, match="max_url_length must be a positive integer"):
            ChemblAssayClient(mock_chembl_client, batch_size=25, max_url_length=0)

    def test_chembl_release_property(self, mock_chembl_client: ChemblClient) -> None:
        """Test chembl_release property."""
        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        assert client.chembl_release is None
        client._chembl_release = "33"
        assert client.chembl_release == "33"

    def test_handshake_enabled(self, mock_chembl_client: MagicMock) -> None:
        """Test handshake when enabled."""
        mock_chembl_client.handshake.return_value = {"chembl_db_version": "33"}
        
        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        result = client.handshake(enabled=True)

        assert mock_chembl_client.handshake.called
        assert result["chembl_db_version"] == "33"
        assert client._chembl_release == "33"

    def test_handshake_disabled(self, mock_chembl_client: MagicMock) -> None:
        """Test handshake when disabled."""
        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        result = client.handshake(enabled=False)

        assert not mock_chembl_client.handshake.called
        assert result == {}
        assert client._chembl_release is None

    def test_handshake_custom_endpoint(self, mock_chembl_client: MagicMock) -> None:
        """Test handshake with custom endpoint."""
        mock_chembl_client.handshake.return_value = {"chembl_db_version": "33"}

        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        client.handshake(endpoint="/custom", enabled=True)

        mock_chembl_client.handshake.assert_called_with("/custom")

    def test_handshake_no_release_in_payload(self, mock_chembl_client: MagicMock) -> None:
        """Test handshake when payload has no release."""
        mock_chembl_client.handshake.return_value = {}

        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        result = client.handshake(enabled=True)

        assert result == {}
        assert client._chembl_release is None

    def test_iterate_all_no_limit(self, mock_chembl_client: MagicMock) -> None:
        """Test iterate_all without limit."""
        mock_chembl_client.paginate.return_value = iter([{"id": 1}, {"id": 2}])

        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        items = list(client.iterate_all())

        assert len(items) == 2
        mock_chembl_client.paginate.assert_called_once()
        call_args = mock_chembl_client.paginate.call_args
        assert call_args[0][0] == "/assay.json"

    def test_iterate_all_with_limit(self, mock_chembl_client: MagicMock) -> None:
        """Test iterate_all with limit."""
        mock_chembl_client.paginate.return_value = iter([{"id": 1}, {"id": 2}, {"id": 3}])

        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        items = list(client.iterate_all(limit=2))

        assert len(items) == 2
        call_args = mock_chembl_client.paginate.call_args
        assert call_args[1]["params"]["limit"] == 2

    def test_iterate_all_with_page_size(self, mock_chembl_client: ChemblClient) -> None:
        """Test iterate_all with custom page_size."""
        mock_chembl_client.paginate.return_value = iter([{"id": 1}])

        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        items = list(client.iterate_all(page_size=10))

        assert len(items) == 1
        call_args = mock_chembl_client.paginate.call_args
        assert call_args[1]["page_size"] == 10

    def test_iterate_all_page_size_capped(self, mock_chembl_client: ChemblClient) -> None:
        """Test that page_size is capped at batch_size."""
        mock_chembl_client.paginate.return_value = iter([{"id": 1}])

        client = ChemblAssayClient(
            mock_chembl_client,
            batch_size=10,
            max_url_length=2000,
        )

        items = list(client.iterate_all(page_size=100))

        call_args = mock_chembl_client.paginate.call_args
        assert call_args[1]["page_size"] == 10  # Capped at batch_size

    def test_iterate_by_ids(self, mock_chembl_client: ChemblClient) -> None:
        """Test iterate_by_ids."""
        mock_chembl_client.paginate.return_value = iter([{"id": "CHEMBL1"}, {"id": "CHEMBL2"}])

        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        items = list(client.iterate_by_ids(["CHEMBL1", "CHEMBL2"]))

        assert len(items) == 2
        mock_chembl_client.paginate.assert_called()
        call_args = mock_chembl_client.paginate.call_args
        assert call_args[1]["params"]["assay_chembl_id__in"] == "CHEMBL1,CHEMBL2"

    def test_iterate_by_ids_chunked(self, mock_chembl_client: ChemblClient) -> None:
        """Test iterate_by_ids with chunking due to URL length."""
        mock_chembl_client.paginate.return_value = iter([{"id": "CHEMBL1"}])

        client = ChemblAssayClient(
            mock_chembl_client,
            batch_size=25,
            max_url_length=50,  # Small URL length to force chunking
        )

        ids = ["CHEMBL" + str(i) for i in range(10)]
        items = list(client.iterate_by_ids(ids))

        # Should chunk due to URL length
        assert mock_chembl_client.paginate.call_count > 1

    def test_iterate_by_ids_invalid_ids(self, mock_chembl_client: ChemblClient) -> None:
        """Test iterate_by_ids with invalid IDs."""
        mock_chembl_client.paginate.return_value = iter([])

        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        items = list(client.iterate_by_ids(["valid", "", None, 123, "also_valid"]))  # type: ignore[list-item]

        # Should only process valid string IDs
        assert mock_chembl_client.paginate.called

    def test_coerce_page_size_none(self, mock_chembl_client: ChemblClient) -> None:
        """Test _coerce_page_size with None."""
        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        result = client._coerce_page_size(None)

        assert result == 25

    def test_coerce_page_size_valid(self, mock_chembl_client: ChemblClient) -> None:
        """Test _coerce_page_size with valid value."""
        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        result = client._coerce_page_size(10)

        assert result == 10

    def test_coerce_page_size_invalid(self, mock_chembl_client: ChemblClient) -> None:
        """Test _coerce_page_size with invalid value."""
        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        result = client._coerce_page_size(0)

        assert result == 25

    def test_coerce_page_size_larger_than_batch(self, mock_chembl_client: ChemblClient) -> None:
        """Test _coerce_page_size with value larger than batch_size."""
        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        result = client._coerce_page_size(100)

        assert result == 25

