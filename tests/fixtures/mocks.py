"""Mock-related pytest fixtures."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable, Iterator
from unittest.mock import MagicMock, patch

import pytest

from bioetl.core.api_client import UnifiedAPIClient


__all__ = [
    "mock_api_client",
    "mock_api_client_factory_patch",
    "mock_chembl_api_client",
    "mock_chembl_client_with_data",
    "mock_chembl_responses_for_endpoint",
    "mock_http_response",
]


@pytest.fixture  # type: ignore[misc]
def mock_api_client() -> MagicMock:
    """Mock UnifiedAPIClient for testing."""
    mock_client = MagicMock(spec=UnifiedAPIClient)
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": "test"}
    mock_response.headers = {}
    mock_client.get.return_value = mock_response
    mock_client.request.return_value = mock_response
    mock_client.request_json.return_value = {"data": "test"}
    return mock_client


@pytest.fixture  # type: ignore[misc]
def mock_http_response() -> MagicMock:
    """Mock HTTP response for testing."""
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {}
    response.headers = {}
    response.raise_for_status = MagicMock()
    return response


@pytest.fixture  # type: ignore[misc]
def mock_chembl_api_client() -> MagicMock:
    """Mock ChEMBL API client with standard responses."""
    mock_client = MagicMock(spec=UnifiedAPIClient)

    mock_status_response = MagicMock()
    mock_status_response.json.return_value = {
        "chembl_release": "33",
        "chembl_db_version": "33",
        "api_version": "1.0",
    }
    mock_status_response.status_code = 200
    mock_status_response.headers = {}

    mock_data_response = MagicMock()
    mock_data_response.json.return_value = {
        "page_meta": {"offset": 0, "limit": 25, "count": 0, "next": None},
        "activities": [],
    }
    mock_data_response.status_code = 200
    mock_data_response.headers = {}

    mock_client.get.side_effect = [mock_status_response, mock_data_response]

    return mock_client


@pytest.fixture  # type: ignore[misc]
def mock_api_client_factory_patch(
    mock_chembl_api_client: MagicMock,
) -> Callable[[MagicMock | None], Iterator[MagicMock]]:
    """Fixture providing a context manager for APIClientFactory.for_source."""

    @contextmanager
    def _factory(mock_client: MagicMock | None = None) -> Iterator[MagicMock]:
        client = mock_client or mock_chembl_api_client
        with patch("bioetl.core.client_factory.APIClientFactory.for_source") as mock_factory:
            mock_factory.return_value = client
            yield mock_factory

    return _factory


@pytest.fixture  # type: ignore[misc]
def mock_chembl_client_with_data(
    sample_activity_data_raw: list[dict[str, Any]]
) -> MagicMock:
    """Mock ChEMBL API client with sample activity data."""
    mock_client = MagicMock(spec=UnifiedAPIClient)

    mock_status_response = MagicMock()
    mock_status_response.json.return_value = {
        "chembl_release": "33",
        "chembl_db_version": "33",
    }
    mock_status_response.status_code = 200
    mock_status_response.headers = {}

    mock_activity_response = MagicMock()
    mock_activity_response.json.return_value = {
        "page_meta": {
            "offset": 0,
            "limit": 25,
            "count": len(sample_activity_data_raw),
            "next": None,
        },
        "activities": sample_activity_data_raw,
    }
    mock_activity_response.status_code = 200
    mock_activity_response.headers = {}

    mock_client.get.side_effect = [mock_status_response, mock_activity_response]

    return mock_client


@pytest.fixture  # type: ignore[misc]
def mock_chembl_responses_for_endpoint() -> Callable[
    [dict[str, Any] | list[dict[str, Any]], str, int | None],
    tuple[MagicMock, MagicMock],
]:
    """Factory fixture creating mock responses for ChEMBL endpoints."""

    def _create_responses(
        endpoint_data: dict[str, Any] | list[dict[str, Any]],
        endpoint_type: str = "activities",
        count: int | None = None,
    ) -> tuple[MagicMock, MagicMock]:
        mock_status_response = MagicMock()
        mock_status_response.json.return_value = {
            "chembl_release": "33",
            "chembl_db_version": "33",
        }
        mock_status_response.status_code = 200
        mock_status_response.headers = {}

        if count is None:
            if isinstance(endpoint_data, list):
                count = len(endpoint_data)
            else:
                count = 1
                endpoint_data = [endpoint_data]
        elif isinstance(endpoint_data, dict):
            endpoint_data = [endpoint_data]

        mock_data_response = MagicMock()
        mock_data_response.json.return_value = {
            "page_meta": {
                "offset": 0,
                "limit": 25,
                "count": count,
                "next": None,
            },
            endpoint_type: endpoint_data if isinstance(endpoint_data, list) else [endpoint_data],
        }
        mock_data_response.status_code = 200
        mock_data_response.headers = {}

        return mock_status_response, mock_data_response

    return _create_responses
