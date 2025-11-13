"""Unit tests for ChemblAssayClient."""

from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock

import pytest

from bioetl.clients.assay.chembl_assay import ChemblAssayClient
from bioetl.clients.chembl import ChemblClient
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

        assert client.chembl_client is mock_chembl_client
        assert client.batch_size == 25
        assert client.max_url_length == 2000

    def test_init_batch_size_capped(self, mock_chembl_client: ChemblClient) -> None:
        """Test that batch_size is capped at 25."""
        client = ChemblAssayClient(
            mock_chembl_client,
            batch_size=100,
            max_url_length=2000,
        )

        assert client.batch_size == 25

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

        handshake_mock = cast(MagicMock, mock_chembl_client.handshake)
        handshake_mock.return_value = {"chembl_db_version": "33"}

        assert client.chembl_release is None
        client.handshake(enabled=True)
        assert client.chembl_release == "33"

    def test_handshake_enabled(self, mock_chembl_client: MagicMock) -> None:
        """Test handshake when enabled."""
        handshake_mock = cast(MagicMock, mock_chembl_client.handshake)
        handshake_mock.return_value = {"chembl_db_version": "33"}

        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        result = client.handshake(enabled=True)

        handshake_mock.assert_called_once()
        assert handshake_mock.call_args.kwargs == {"endpoint": "/status", "enabled": True}
        assert result["chembl_db_version"] == "33"
        assert client.chembl_release == "33"

    def test_handshake_disabled(self, mock_chembl_client: MagicMock) -> None:
        """Test handshake when disabled."""
        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        handshake_mock = cast(MagicMock, mock_chembl_client.handshake)

        result = client.handshake(enabled=False)

        handshake_mock.assert_not_called()
        assert result == {}
        assert client.chembl_release is None

    def test_handshake_custom_endpoint(self, mock_chembl_client: MagicMock) -> None:
        """Test handshake with custom endpoint."""
        handshake_mock = cast(MagicMock, mock_chembl_client.handshake)
        handshake_mock.return_value = {"chembl_db_version": "33"}

        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        client.handshake(endpoint="/custom", enabled=True)

        handshake_mock.assert_called_with(endpoint="/custom", enabled=True)

    def test_handshake_no_release_in_payload(self, mock_chembl_client: MagicMock) -> None:
        """Test handshake when payload has no release."""
        handshake_mock = cast(MagicMock, mock_chembl_client.handshake)
        handshake_mock.return_value = {}

        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        result = client.handshake(enabled=True)

        assert result == {}
        assert client.chembl_release is None

    def test_iterate_all_no_limit(self, mock_chembl_client: MagicMock) -> None:
        """Test iterate_all without limit."""
        paginate_mock = cast(MagicMock, mock_chembl_client.paginate)
        paginate_mock.return_value = iter([{"id": 1}, {"id": 2}])

        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        items = list(client.iterate_all())

        assert len(items) == 2
        paginate_mock.assert_called_once()
        call_args = paginate_mock.call_args
        assert call_args is not None
        assert call_args.args[0] == "/assay.json"

    def test_iterate_all_with_limit(self, mock_chembl_client: MagicMock) -> None:
        """Test iterate_all with limit."""
        paginate_mock = cast(MagicMock, mock_chembl_client.paginate)
        paginate_mock.return_value = iter([{"id": 1}, {"id": 2}, {"id": 3}])

        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        items = list(client.iterate_all(limit=2))

        assert len(items) == 2
        call_args = paginate_mock.call_args
        assert call_args is not None
        assert call_args.kwargs["params"]["limit"] == 2

    def test_iterate_all_with_page_size(self, mock_chembl_client: ChemblClient) -> None:
        """Test iterate_all with custom page_size."""
        paginate_mock = cast(MagicMock, mock_chembl_client.paginate)
        paginate_mock.return_value = iter([{"id": 1}])

        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        items = list(client.iterate_all(page_size=10))

        assert len(items) == 1
        call_args = paginate_mock.call_args
        assert call_args is not None
        assert call_args.kwargs["page_size"] == 10

    def test_iterate_all_page_size_capped(self, mock_chembl_client: ChemblClient) -> None:
        """Test that page_size is capped at batch_size."""
        paginate_mock = cast(MagicMock, mock_chembl_client.paginate)
        paginate_mock.return_value = iter([{"id": 1}])

        client = ChemblAssayClient(
            mock_chembl_client,
            batch_size=10,
            max_url_length=2000,
        )

        items = list(client.iterate_all(page_size=100))

        assert len(items) == 1
        call_args = paginate_mock.call_args
        assert call_args is not None
        assert call_args.kwargs["page_size"] == 10  # Capped at batch_size

    def test_iterate_by_ids(self, mock_chembl_client: ChemblClient) -> None:
        """Test iterate_by_ids."""
        paginate_mock = cast(MagicMock, mock_chembl_client.paginate)
        paginate_mock.return_value = iter([{"id": "CHEMBL1"}, {"id": "CHEMBL2"}])

        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        items = list(client.iterate_by_ids(["CHEMBL1", "CHEMBL2"]))

        assert len(items) == 2
        paginate_mock.assert_called()
        call_args = paginate_mock.call_args
        assert call_args is not None
        assert call_args.kwargs["params"]["assay_chembl_id__in"] == "CHEMBL1,CHEMBL2"

    def test_iterate_by_ids_chunked(self, mock_chembl_client: ChemblClient) -> None:
        """Test iterate_by_ids with chunking due to URL length."""
        paginate_mock = cast(MagicMock, mock_chembl_client.paginate)
        paginate_mock.return_value = iter([{"id": "CHEMBL1"}])

        client = ChemblAssayClient(
            mock_chembl_client,
            batch_size=25,
            max_url_length=50,  # Small URL length to force chunking
        )

        ids = ["CHEMBL" + str(i) for i in range(10)]
        _ = list(client.iterate_by_ids(ids))
        # Should chunk due to URL length
        assert paginate_mock.call_count > 1

    def test_iterate_by_ids_invalid_ids(self, mock_chembl_client: ChemblClient) -> None:
        """Test iterate_by_ids with invalid IDs."""
        paginate_mock = cast(MagicMock, mock_chembl_client.paginate)
        paginate_mock.return_value = iter([])

        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        _ = list(client.iterate_by_ids(["valid", "", " ", "also_valid"]))

        paginate_mock.assert_called()
        call_args = paginate_mock.call_args
        assert call_args is not None
        assert call_args.kwargs["params"]["assay_chembl_id__in"] == "valid,also_valid"

    def test_coerce_page_size_none(self, mock_chembl_client: ChemblClient) -> None:
        """Test _coerce_page_size with None."""
        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        paginate_mock = cast(MagicMock, mock_chembl_client.paginate)
        paginate_mock.return_value = iter([])

        _ = list(client.iterate_all())

        call_args = paginate_mock.call_args
        assert call_args is not None
        assert call_args.kwargs["page_size"] == client.batch_size

    def test_coerce_page_size_valid(self, mock_chembl_client: ChemblClient) -> None:
        """Test _coerce_page_size with valid value."""
        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        paginate_mock = cast(MagicMock, mock_chembl_client.paginate)
        paginate_mock.return_value = iter([])

        _ = list(client.iterate_all(page_size=10))

        call_args = paginate_mock.call_args
        assert call_args is not None
        assert call_args.kwargs["page_size"] == 10

    def test_coerce_page_size_invalid(self, mock_chembl_client: ChemblClient) -> None:
        """Test _coerce_page_size with invalid value."""
        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        paginate_mock = cast(MagicMock, mock_chembl_client.paginate)
        paginate_mock.return_value = iter([])

        _ = list(client.iterate_all(page_size=0))

        call_args = paginate_mock.call_args
        assert call_args is not None
        assert call_args.kwargs["page_size"] == client.batch_size

    def test_coerce_page_size_larger_than_batch(self, mock_chembl_client: ChemblClient) -> None:
        """Test _coerce_page_size with value larger than batch_size."""
        client = ChemblAssayClient(
            mock_chembl_client,  # type: ignore[arg-type]
            batch_size=25,
            max_url_length=2000,
        )

        paginate_mock = cast(MagicMock, mock_chembl_client.paginate)
        paginate_mock.return_value = iter([])

        _ = list(client.iterate_all(page_size=100))

        call_args = paginate_mock.call_args
        assert call_args is not None
        assert call_args.kwargs["page_size"] == client.batch_size
