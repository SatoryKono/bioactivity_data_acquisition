"""Unit tests for ChemblClient."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from bioetl.clients.client_chembl import ChemblClient, _resolve_status_endpoint
from bioetl.clients.client_chembl_entity_base import ChemblEntityClientProtocol
from bioetl.core.http.api_client import UnifiedAPIClient


@pytest.fixture
def mock_api_client() -> MagicMock:
    """Mock UnifiedAPIClient for testing."""
    mock_client = MagicMock(spec=UnifiedAPIClient)
    return mock_client


@pytest.fixture
def mock_response() -> MagicMock:
    """Mock HTTP response for testing."""
    response = MagicMock()
    response.json.return_value = {}
    return response


@pytest.mark.unit
class TestChemblClient:
    """Test suite for ChemblClient."""

    def test_init(self, mock_api_client: MagicMock) -> None:
        """Test ChemblClient initialization."""
        client = ChemblClient(mock_api_client)
        assert client._client == mock_api_client
        assert client._status_cache == {}

    def test_handshake_cache(self, mock_api_client: MagicMock, mock_response: MagicMock) -> None:
        """Test handshake caching."""
        mock_response.json.return_value = {
            "chembl_db_version": "33",
            "api_version": "1.0",
        }
        mock_api_client.get.return_value = mock_response

        client = ChemblClient(mock_api_client)
        default_endpoint = _resolve_status_endpoint()

        result1 = client.handshake()
        result2 = client.handshake()

        # Should only call API once
        assert mock_api_client.get.call_count == 1
        assert mock_api_client.get.call_args_list[0][0][0] == default_endpoint
        assert result1 == result2
        assert result1["chembl_db_version"] == "33"
        assert default_endpoint in client._status_cache

    def test_handshake_different_endpoints(
        self, mock_api_client: MagicMock, mock_response: MagicMock
    ) -> None:
        """Test handshake with different endpoints."""
        mock_response.json.return_value = {"chembl_db_version": "33"}
        mock_api_client.get.return_value = mock_response

        client = ChemblClient(mock_api_client)
        default_endpoint = _resolve_status_endpoint()
        client.handshake()
        client.handshake("/version")

        # Should call API twice for different endpoints
        assert mock_api_client.get.call_count == 2
        assert default_endpoint in client._status_cache
        assert "/version" in client._status_cache

    def test_handshake_default_endpoint_from_config(
        self,
        mock_api_client: MagicMock,
        mock_response: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ensure handshake() uses the endpoint resolved from configuration."""

        mock_response.json.return_value = {
            "chembl_db_version": "34",
            "api_version": "2.0",
        }
        mock_api_client.get.return_value = mock_response

        configured_endpoint = "/configured-status.json"
        monkeypatch.setattr(
            "bioetl.clients.client_chembl._resolve_status_endpoint",
            lambda: configured_endpoint,
        )

        client = ChemblClient(mock_api_client)
        payload = client.handshake()

        assert payload["chembl_db_version"] == "34"
        assert mock_api_client.get.call_args_list[0][0][0] == configured_endpoint
        assert configured_endpoint in client._status_cache

    def test_paginate_single_page(
        self, mock_api_client: MagicMock, mock_response: MagicMock
    ) -> None:
        """Test pagination with single page."""
        mock_response.json.return_value = {
            "page_meta": {"next": None},
            "activities": [{"id": 1}, {"id": 2}],
        }
        mock_api_client.get.return_value = mock_response

        client = ChemblClient(mock_api_client)
        items = list(client.paginate("/activities.json", page_size=200))

        assert len(items) == 2
        assert items[0]["id"] == 1
        assert items[1]["id"] == 2

    def test_paginate_multiple_pages(self, mock_api_client: MagicMock) -> None:
        """Test pagination with multiple pages."""
        response1 = MagicMock()
        response1.json.return_value = {
            "page_meta": {"next": "/activities.json?offset=2"},
            "activities": [{"id": 1}, {"id": 2}],
        }
        response2 = MagicMock()
        response2.json.return_value = {
            "page_meta": {"next": None},
            "activities": [{"id": 3}],
        }

        # First call for handshake, then two calls for pagination
        mock_api_client.get.side_effect = [response1, response1, response2]

        client = ChemblClient(mock_api_client)
        items = list(client.paginate("/activities.json", page_size=2))

        assert len(items) == 3
        assert mock_api_client.get.call_count >= 2

    def test_paginate_with_params(
        self, mock_api_client: MagicMock, mock_response: MagicMock
    ) -> None:
        """Test pagination with custom parameters."""
        mock_response.json.return_value = {
            "page_meta": {"next": None},
            "activities": [{"id": 1}],
        }
        mock_api_client.get.return_value = mock_response

        client = ChemblClient(mock_api_client)
        items = list(client.paginate("/activities.json", params={"filter": "value"}, page_size=100))

        assert len(items) == 1
        # Should pass params to first request (after handshake)
        # Find the call to /activities.json (skip handshake call to default status endpoint)
        activity_calls = [
            call for call in mock_api_client.get.call_args_list if call[0][0] == "/activities.json"
        ]
        assert len(activity_calls) > 0
        call_args = activity_calls[0]
        assert call_args[1]["params"]["filter"] == "value"
        assert call_args[1]["params"]["limit"] == 100

    def test_paginate_with_custom_items_key(
        self, mock_api_client: MagicMock, mock_response: MagicMock
    ) -> None:
        """Test pagination with custom items key."""
        mock_response.json.return_value = {
            "page_meta": {"next": None},
            "custom_items": [{"id": 1}, {"id": 2}],
        }
        mock_api_client.get.return_value = mock_response

        client = ChemblClient(mock_api_client)
        items = list(client.paginate("/endpoint.json", items_key="custom_items"))

        assert len(items) == 2

    def test_paginate_with_empty_items(
        self, mock_api_client: MagicMock, mock_response: MagicMock
    ) -> None:
        """Test pagination with empty items."""
        mock_response.json.return_value = {
            "page_meta": {"next": None},
            "activities": [],
        }
        mock_api_client.get.return_value = mock_response

        client = ChemblClient(mock_api_client)
        items = list(client.paginate("/activities.json"))

        assert len(items) == 0

    def test_paginate_with_non_sequence_items(
        self, mock_api_client: MagicMock, mock_response: MagicMock
    ) -> None:
        """Test pagination when items_key points to non-sequence."""
        mock_response.json.return_value = {
            "page_meta": {"next": None},
            "activities": "not a list",
        }
        mock_api_client.get.return_value = mock_response

        client = ChemblClient(mock_api_client)
        items = list(client.paginate("/activities.json", items_key="activities"))

        assert len(items) == 0

    def test_paginate_auto_detect_items(
        self, mock_api_client: MagicMock, mock_response: MagicMock
    ) -> None:
        """Test pagination auto-detecting items when items_key is None."""
        mock_response.json.return_value = {
            "page_meta": {"next": None},
            "records": [{"id": 1}, {"id": 2}],
        }
        mock_api_client.get.return_value = mock_response

        client = ChemblClient(mock_api_client)
        items = list(client.paginate("/endpoint.json"))

        assert len(items) == 2

    def test_paginate_handshake_called(
        self, mock_api_client: MagicMock, mock_response: MagicMock
    ) -> None:
        """Test that handshake is called before pagination."""
        mock_response.json.return_value = {
            "page_meta": {"next": None},
            "activities": [],
        }
        mock_api_client.get.return_value = mock_response

        client = ChemblClient(mock_api_client)
        list(client.paginate("/activities.json"))

        # Should call handshake (which calls get) and then paginate
        assert mock_api_client.get.call_count >= 1

    def test_fetch_entities_helper_materializes_iterable(
        self, mock_api_client: MagicMock
    ) -> None:
        """The helper must materialize the incoming iterable before delegating."""

        client = ChemblClient(mock_api_client)
        entity = MagicMock(spec=ChemblEntityClientProtocol)
        expected_result = object()
        entity.fetch_by_ids.return_value = expected_result

        ids_iterable = (identifier for identifier in ["CHEMBL1", "CHEMBL2"])
        fields = ("field_a", "field_b")
        page_limit = 5

        client._assay_entity = entity  # type: ignore[assignment]

        result = client._fetch_entities(
            "assay",
            ids_iterable,
            fields=fields,
            page_limit=page_limit,
        )

        entity.fetch_by_ids.assert_called_once_with(
            ("CHEMBL1", "CHEMBL2"),
            fields=fields,
            page_limit=page_limit,
        )
        assert result is expected_result

    def test_fetch_assays_by_ids_delegates_to_helper(
        self, mock_api_client: MagicMock
    ) -> None:
        """Entity-specific wrapper should delegate to the shared helper."""

        client = ChemblClient(mock_api_client)
        sentinel = object()
        client._fetch_entities = MagicMock(return_value=sentinel)  # type: ignore[method-assign]

        result = client.fetch_assays_by_ids(["CHEMBL1"], fields=None, page_limit=None)

        client._fetch_entities.assert_called_once_with(
            "assay",
            ["CHEMBL1"],
            fields=None,
            page_limit=None,
        )
        assert result is sentinel

    def test_fetch_molecules_by_ids_delegates_to_helper(
        self, mock_api_client: MagicMock
    ) -> None:
        """Molecule wrapper should delegate to the shared helper."""

        client = ChemblClient(mock_api_client)
        sentinel = object()
        client._fetch_entities = MagicMock(return_value=sentinel)  # type: ignore[method-assign]

        result = client.fetch_molecules_by_ids(["CHEMBL123"], fields=None, page_limit=3)

        client._fetch_entities.assert_called_once_with(
            "molecule",
            ["CHEMBL123"],
            fields=None,
            page_limit=3,
        )
        assert result is sentinel

    def test_fetch_entities_unknown_entity_raises(
        self, mock_api_client: MagicMock
    ) -> None:
        """Unknown entity names should surface a descriptive AttributeError."""

        client = ChemblClient(mock_api_client)

        with pytest.raises(AttributeError) as excinfo:
            client._fetch_entities("missing", ["CHEMBL1"])

        assert "missing" in str(excinfo.value)
