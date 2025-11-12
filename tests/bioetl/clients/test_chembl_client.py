"""Unit tests for ChemblClient."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from bioetl.clients.client_chembl import ChemblClient
from bioetl.core.api_client import UnifiedAPIClient


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
        result1 = client.handshake("/status")
        result2 = client.handshake("/status")

        # Should only call API once
        assert mock_api_client.get.call_count == 1
        assert result1 == result2
        assert result1["chembl_db_version"] == "33"
        assert "/status" in client._status_cache

    def test_handshake_different_endpoints(
        self, mock_api_client: MagicMock, mock_response: MagicMock
    ) -> None:
        """Test handshake with different endpoints."""
        mock_response.json.return_value = {"chembl_db_version": "33"}
        mock_api_client.get.return_value = mock_response

        client = ChemblClient(mock_api_client)
        client.handshake("/status")
        client.handshake("/version")

        # Should call API twice for different endpoints
        assert mock_api_client.get.call_count == 2
        assert "/status" in client._status_cache
        assert "/version" in client._status_cache

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
        # Find the call to /activities.json (skip handshake call to /status)
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
