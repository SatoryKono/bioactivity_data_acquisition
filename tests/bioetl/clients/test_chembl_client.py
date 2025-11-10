"""Unit tests for ChemblClient."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from _pytest.python_api import ApproxBase
from requests import HTTPError

from bioetl.clients.chembl import ChemblClient
from bioetl.config.models.models import (
    ChemblClientConfig,
    ChemblPreflightConfig,
    ChemblPreflightRetryConfig,
)
from bioetl.core.api_client import UnifiedAPIClient

ApproxFloatCallable = Callable[[float], ApproxBase]
approx_float = cast(ApproxFloatCallable, pytest.approx)


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


@pytest.fixture
def chembl_client_settings() -> ChemblClientConfig:
    """Fixture providing preflight-enabled client settings for tests."""
    return ChemblClientConfig(
        preflight=ChemblPreflightConfig(
            enabled=True,
            retry=ChemblPreflightRetryConfig(total=1, backoff_factor=0.5),
            budget_seconds=5.0,
        )
    )


@pytest.mark.unit
class TestChemblClient:
    """Test suite for ChemblClient."""

    def test_init(self, mock_api_client: MagicMock) -> None:
        """Test ChemblClient initialization."""
        client = ChemblClient(mock_api_client)
        client_state = vars(client)
        assert client_state["_client"] == mock_api_client
        assert client_state["_status_cache"] == {}

    def test_handshake_cache(
        self,
        mock_api_client: MagicMock,
        mock_response: MagicMock,
        chembl_client_settings: ChemblClientConfig,
    ) -> None:
        """Test handshake caching."""
        mock_response.json.return_value = {
            "chembl_db_version": "33",
            "api_version": "1.0",
        }
        mock_api_client.get.return_value = mock_response

        client = ChemblClient(mock_api_client, settings=chembl_client_settings)
        result1 = client.handshake("/status")
        result2 = client.handshake("/status")

        # Should only call API once
        assert mock_api_client.get.call_count == 1
        default_timeout = vars(client)["_default_timeout"]
        timeout_tuple = mock_api_client.get.call_args.kwargs["timeout"]
        assert timeout_tuple[0] == approx_float(default_timeout[0])
        assert timeout_tuple[1] <= default_timeout[1] + 1e-6
        assert mock_api_client.get.call_args.kwargs["retry_strategy"] == "none"
        assert result1 == result2
        assert result1["chembl_db_version"] == "33"
        assert "/status" in vars(client)["_status_cache"]

    def test_handshake_different_endpoints(
        self,
        mock_api_client: MagicMock,
        mock_response: MagicMock,
        chembl_client_settings: ChemblClientConfig,
    ) -> None:
        """Test handshake with different endpoints."""
        mock_response.json.return_value = {"chembl_db_version": "33"}
        mock_api_client.get.return_value = mock_response

        client = ChemblClient(mock_api_client, settings=chembl_client_settings)
        client.handshake("/status")
        client.handshake("/version")

        assert mock_api_client.get.call_count >= 1
        handshake_timeout = vars(client)["_default_timeout"]
        for call_args in mock_api_client.get.call_args_list:
            timeout_tuple = call_args.kwargs["timeout"]
            assert timeout_tuple[0] == approx_float(handshake_timeout[0])
            assert timeout_tuple[1] <= handshake_timeout[1] + 1e-6
            assert call_args.kwargs["retry_strategy"] == "none"
        status_cache = vars(client)["_status_cache"]
        assert "/status" in status_cache
        assert "/version" in status_cache

    def test_handshake_falls_back_to_json_endpoint(
        self,
        mock_api_client: MagicMock,
        chembl_client_settings: ChemblClientConfig,
    ) -> None:
        """Ensure handshake retries with ``.json`` suffix when the plain endpoint fails."""

        success_response = MagicMock()
        success_response.json.return_value = {"chembl_db_version": "34", "api_version": "2"}

        mock_api_client.get.side_effect = [
            HTTPError("boom"),
            HTTPError("boom"),
            success_response,
        ]

        client = ChemblClient(mock_api_client, settings=chembl_client_settings)
        payload = client.handshake("/status")

        assert payload["chembl_db_version"] == "34"
        assert vars(client)["_chembl_release"] == "34"
        assert mock_api_client.get.call_count == 3
        assert mock_api_client.get.call_args_list[0].args[0] == "/status"
        assert mock_api_client.get.call_args_list[2].args[0] == "/status.json"
        handshake_timeout = vars(client)["_default_timeout"]
        for invocation in mock_api_client.get.call_args_list:
            timeout_tuple = invocation.kwargs["timeout"]
            assert timeout_tuple[0] == approx_float(handshake_timeout[0])
            assert timeout_tuple[1] <= handshake_timeout[1] + 1e-6
            assert invocation.kwargs["retry_strategy"] == "none"
        status_cache = vars(client)["_status_cache"]
        assert "/status" in status_cache
        assert "/status.json" in status_cache

    def test_handshake_returns_empty_payload_when_all_endpoints_fail(
        self,
        mock_api_client: MagicMock,
        chembl_client_settings: ChemblClientConfig,
    ) -> None:
        """Handshake should degrade gracefully when all candidate endpoints fail."""

        def _raise_http_error(*_args: Any, **_kwargs: Any) -> None:
            raise HTTPError("boom")

        mock_api_client.get.side_effect = _raise_http_error

        client = ChemblClient(mock_api_client, settings=chembl_client_settings)
        payload = client.handshake("/status")

        assert payload == {}
        assert mock_api_client.get.call_count >= 1
        assert "/status" in vars(client)["_status_cache"]
        first_call = mock_api_client.get.call_args_list[0]
        timeout_tuple = first_call.kwargs["timeout"]
        default_timeout = vars(client)["_default_timeout"]
        assert timeout_tuple[0] == approx_float(default_timeout[0])
        assert timeout_tuple[1] <= default_timeout[1] + 1e-6
        assert first_call.kwargs["retry_strategy"] == "none"

    def test_handshake_custom_timeout(
        self,
        mock_api_client: MagicMock,
        mock_response: MagicMock,
        chembl_client_settings: ChemblClientConfig,
    ) -> None:
        """Explicit timeout overrides should be honoured per invocation."""

        mock_response.json.return_value = {"chembl_db_version": "35"}
        mock_api_client.get.return_value = mock_response

        client = ChemblClient(
            mock_api_client,
            settings=chembl_client_settings,
            handshake_timeout=5.0,
        )
        payload = client.handshake("/status", timeout=1.5)

        assert payload["chembl_db_version"] == "35"
        assert mock_api_client.get.call_count == 1
        assert mock_api_client.get.call_args.kwargs["timeout"] == (1.5, 1.5)
        assert mock_api_client.get.call_args.kwargs["retry_strategy"] == "none"
        assert vars(client)["_default_timeout"] == (3.05, 5.0)

    def test_handshake_failure_cached(
        self,
        mock_api_client: MagicMock,
        chembl_client_settings: ChemblClientConfig,
    ) -> None:
        """Repeated handshake failures should be cached for the circuit open window."""

        client = ChemblClient(
            mock_api_client,
            settings=ChemblClientConfig(
                preflight=ChemblPreflightConfig(
                    enabled=True,
                    retry=ChemblPreflightRetryConfig(total=0, backoff_factor=0.1),
                    budget_seconds=5.0,
                )
            ),
        )

        mock_api_client.get.side_effect = HTTPError("boom")

        first_payload = client.handshake("/status")
        assert first_payload == {}
        expected_attempts = len(
            (
                "/status",
                "/status.json",
                "/data/version",
                "/data/version.json",
                "/data",
                "/data.json",
            )
        )
        assert mock_api_client.get.call_count == expected_attempts

        mock_api_client.get.reset_mock()
        second_payload = client.handshake("/status")

        assert second_payload == {}
        assert mock_api_client.get.call_count == 0

    def test_handshake_cache_ttl_expiry(
        self,
        mock_api_client: MagicMock,
        mock_response: MagicMock,
    ) -> None:
        """Expired handshake cache should trigger a fresh request."""

        mock_response.json.return_value = {"chembl_db_version": "99"}
        mock_api_client.get.return_value = mock_response

        client = ChemblClient(
            mock_api_client,
            settings=ChemblClientConfig(
                preflight=ChemblPreflightConfig(
                    enabled=True,
                    retry=ChemblPreflightRetryConfig(total=0, backoff_factor=0.1),
                    cache_ttl_seconds=0.001,
                    budget_seconds=5.0,
                )
            ),
        )

        result_first = client.handshake("/status")
        assert result_first["chembl_db_version"] == "99"
        assert mock_api_client.get.call_count == 1

        mock_api_client.get.reset_mock()
        entry = vars(client)["_status_cache"]["/status"]
        entry.expires_at = time.monotonic() - 1
        mock_api_client.get.return_value = mock_response
        result_second = client.handshake("/status")

        assert result_second["chembl_db_version"] == "99"
        assert mock_api_client.get.call_count == 1

    def test_handshake_fallback_to_version_endpoint(
        self,
        mock_api_client: MagicMock,
        chembl_client_settings: ChemblClientConfig,
    ) -> None:
        """Handshake should escalate to /data/version when status endpoints fail."""

        success_response = MagicMock()
        success_response.json.return_value = {"chembl_db_version": "45"}
        mock_api_client.get.side_effect = [
            HTTPError("boom"),
            HTTPError("boom"),
            HTTPError("boom"),
            HTTPError("boom"),
            success_response,
        ]

        client = ChemblClient(mock_api_client, settings=chembl_client_settings)
        payload = client.handshake("/status")

        assert payload["chembl_db_version"] == "45"
        assert mock_api_client.get.call_args_list[-1].args[0] == "/data/version"
        assert mock_api_client.get.call_count == 5

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

        mock_api_client.get.side_effect = [response1, response2]

        client = ChemblClient(mock_api_client)
        items = list(client.paginate("/activities.json", page_size=2))

        assert len(items) == 3
        assert mock_api_client.get.call_count == 2

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
            invocation
            for invocation in mock_api_client.get.call_args_list
            if invocation[0][0] == "/activities.json"
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
        vars(client)["_assay_entity"] = mock_entity

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
        vars(client)["_molecule_entity"] = mock_entity

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
        vars(client)["_data_validity_entity"] = mock_entity

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
        vars(client)["_compound_record_entity"] = mock_entity

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
        vars(client)["_document_term_entity"] = mock_entity

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
        vars(client)["_assay_class_map_entity"] = mock_entity

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
        vars(client)["_assay_parameters_entity"] = mock_entity

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
        vars(client)["_assay_classification_entity"] = mock_entity

        ids = ["CLS"]
        fields = ["field"]
        result = client.fetch_assay_classifications_by_class_ids(ids, fields, page_limit=60)

        mock_entity.fetch_by_ids.assert_called_once_with(ids, fields, 60)
        assert result == expected

    def test_fetch_entity_requires_fetch_by_ids(self, mock_api_client: MagicMock) -> None:
        """Helper should raise when entity lacks fetch_by_ids attribute."""
        client = ChemblClient(mock_api_client)

        with pytest.raises(AttributeError):
            vars(client)["_assay_entity"] = object()
            client.fetch_assays_by_ids([], [])
