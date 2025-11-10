"""Unit tests for ChemblClient."""

from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest

from requests import HTTPError

from bioetl.clients.chembl import ChemblClient
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
        assert mock_api_client.get.call_args.kwargs["timeout"] == client._handshake_timeout
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
        for call_args in mock_api_client.get.call_args_list:
            assert call_args.kwargs["timeout"] == client._handshake_timeout
        assert "/status" in client._status_cache
        assert "/version" in client._status_cache

    def test_handshake_falls_back_to_json_endpoint(
        self, mock_api_client: MagicMock
    ) -> None:
        """Ensure handshake retries with ``.json`` suffix when the plain endpoint fails."""

        success_response = MagicMock()
        success_response.json.return_value = {"chembl_db_version": "34", "api_version": "2"}

        mock_api_client.get.side_effect = [HTTPError("boom"), success_response]

        client = ChemblClient(mock_api_client)
        payload = client.handshake("/status")

        assert payload["chembl_db_version"] == "34"
        assert client._chembl_release == "34"
        assert mock_api_client.get.call_count == 2
        assert mock_api_client.get.call_args_list[0].args[0] == "/status"
        assert mock_api_client.get.call_args_list[1].args[0] == "/status.json"
        assert mock_api_client.get.call_args_list[0].kwargs["timeout"] == client._handshake_timeout
        assert mock_api_client.get.call_args_list[1].kwargs["timeout"] == client._handshake_timeout
        assert "/status" in client._status_cache
        assert "/status.json" in client._status_cache

    def test_handshake_returns_empty_payload_when_all_endpoints_fail(
        self, mock_api_client: MagicMock
    ) -> None:
        """Handshake should degrade gracefully when all candidate endpoints fail."""

        mock_api_client.get.side_effect = [HTTPError("boom"), HTTPError("boom")]

        client = ChemblClient(mock_api_client)
        payload = client.handshake("/status")

        assert payload == {}
        assert mock_api_client.get.call_count == 2
        assert "/status" in client._status_cache
        for call_args in mock_api_client.get.call_args_list:
            assert call_args.kwargs["timeout"] == client._handshake_timeout

    def test_handshake_custom_timeout(
        self, mock_api_client: MagicMock, mock_response: MagicMock
    ) -> None:
        """Explicit timeout overrides should be honoured per invocation."""

        mock_response.json.return_value = {"chembl_db_version": "35"}
        mock_api_client.get.return_value = mock_response

        client = ChemblClient(mock_api_client, handshake_timeout=5.0)
        payload = client.handshake("/status", timeout=1.5)

        assert payload["chembl_db_version"] == "35"
        assert mock_api_client.get.call_count == 1
        assert mock_api_client.get.call_args.kwargs["timeout"] == (1.5, 1.5)
        assert client._handshake_timeout == (3.05, 5.0)

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

    def test_fetch_assays_routes_to_entity(self, mock_api_client: MagicMock) -> None:
        """Ensure fetch_assays_by_ids delegates to the assay entity."""
        client = ChemblClient(mock_api_client)
        mock_entity = MagicMock()
        expected = {"A": {"id": "A"}}
        mock_entity.fetch_by_ids.return_value = expected
        client._assay_entity = mock_entity  # type: ignore[assignment]

        ids = ["A"]
        fields = ["field1"]
        result = client.fetch_assays_by_ids(ids, fields, page_limit=25)

        mock_entity.fetch_by_ids.assert_called_once_with(ids, fields, 25)
        assert result == expected

    def test_fetch_molecules_routes_to_entity(self, mock_api_client: MagicMock) -> None:
        """Ensure fetch_molecules_by_ids delegates to the molecule entity."""
        client = ChemblClient(mock_api_client)
        mock_entity = MagicMock()
        expected = {"M1": {"id": "M1"}}
        mock_entity.fetch_by_ids.return_value = expected
        client._molecule_entity = mock_entity  # type: ignore[assignment]

        ids = ["M1"]
        fields = ["field1"]
        result = client.fetch_molecules_by_ids(ids, fields, page_limit=50)

        mock_entity.fetch_by_ids.assert_called_once_with(ids, fields, 50)
        assert result == expected

    def test_fetch_data_validity_routes_to_entity(
        self, mock_api_client: MagicMock
    ) -> None:
        """Ensure fetch_data_validity_lookup delegates to the entity client."""
        client = ChemblClient(mock_api_client)
        mock_entity = MagicMock()
        expected = {"comment": {"id": 1}}
        mock_entity.fetch_by_ids.return_value = expected
        client._data_validity_entity = mock_entity  # type: ignore[assignment]

        comments = ["comment"]
        fields = ["field"]
        result = client.fetch_data_validity_lookup(comments, fields, page_limit=10)

        mock_entity.fetch_by_ids.assert_called_once_with(comments, fields, 10)
        assert result == expected

    def test_fetch_compound_records_routes_to_entity(
        self, mock_api_client: MagicMock
    ) -> None:
        """Ensure fetch_compound_records_by_pairs delegates to compound entity."""
        client = ChemblClient(mock_api_client)
        mock_entity = MagicMock()
        expected = {("M", "D"): {"id": 1}}
        mock_entity.fetch_by_pairs.return_value = expected
        client._compound_record_entity = mock_entity  # type: ignore[assignment]

        pairs = [("M", "D")]
        fields = ["field"]
        result = client.fetch_compound_records_by_pairs(pairs, fields, page_limit=5)

        mock_entity.fetch_by_pairs.assert_called_once_with(pairs, fields, 5)
        assert result == expected

    def test_fetch_document_terms_routes_to_entity(
        self, mock_api_client: MagicMock
    ) -> None:
        """Ensure fetch_document_terms_by_ids delegates to document term entity."""
        client = ChemblClient(mock_api_client)
        mock_entity = MagicMock()
        expected = {"DOC": [{"id": 1}]}
        mock_entity.fetch_by_ids.return_value = expected
        client._document_term_entity = mock_entity  # type: ignore[assignment]

        ids = ["DOC"]
        fields = ["field"]
        result = client.fetch_document_terms_by_ids(ids, fields, page_limit=15)

        mock_entity.fetch_by_ids.assert_called_once_with(ids, fields, 15)
        assert result == expected

    def test_fetch_assay_class_map_routes_to_entity(
        self, mock_api_client: MagicMock
    ) -> None:
        """Ensure fetch_assay_class_map_by_assay_ids delegates correctly."""
        client = ChemblClient(mock_api_client)
        mock_entity = MagicMock()
        expected = {"ASSAY": [{"id": 1}]}
        mock_entity.fetch_by_ids.return_value = expected
        client._assay_class_map_entity = mock_entity  # type: ignore[assignment]

        ids = ["ASSAY"]
        fields = ["field"]
        result = client.fetch_assay_class_map_by_assay_ids(ids, fields, page_limit=30)

        mock_entity.fetch_by_ids.assert_called_once_with(ids, fields, 30)
        assert result == expected

    def test_fetch_assay_parameters_routes_to_entity(
        self, mock_api_client: MagicMock
    ) -> None:
        """Ensure fetch_assay_parameters_by_assay_ids passes active_only flag."""
        client = ChemblClient(mock_api_client)
        mock_entity = MagicMock()
        expected = {"ASSAY": [{"id": 1}]}
        mock_entity.fetch_by_ids.return_value = expected
        client._assay_parameters_entity = mock_entity  # type: ignore[assignment]

        ids = ["ASSAY"]
        fields = ["field"]
        result = client.fetch_assay_parameters_by_assay_ids(
            ids, fields, page_limit=40, active_only=False
        )

        mock_entity.fetch_by_ids.assert_called_once_with(ids, fields, 40, active_only=False)
        assert result == expected

    def test_fetch_assay_classifications_routes_to_entity(
        self, mock_api_client: MagicMock
    ) -> None:
        """Ensure fetch_assay_classifications_by_class_ids delegates correctly."""
        client = ChemblClient(mock_api_client)
        mock_entity = MagicMock()
        expected = {"CLS": {"id": 1}}
        mock_entity.fetch_by_ids.return_value = expected
        client._assay_classification_entity = mock_entity  # type: ignore[assignment]

        ids = ["CLS"]
        fields = ["field"]
        result = client.fetch_assay_classifications_by_class_ids(ids, fields, page_limit=60)

        mock_entity.fetch_by_ids.assert_called_once_with(ids, fields, 60)
        assert result == expected

    def test_fetch_entity_requires_fetch_by_ids(self, mock_api_client: MagicMock) -> None:
        """Helper should raise when entity lacks fetch_by_ids attribute."""
        client = ChemblClient(mock_api_client)

        with pytest.raises(AttributeError):
            client._fetch_entity(object(), [], [], 10)
