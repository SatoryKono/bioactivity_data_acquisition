"""Test functionality of base enricher client."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from bioetl.clients import exceptions as client_exceptions
from bioetl.clients.enrichers.base import BaseEnricherClient


class _DummyApiClient:
    """Dummy API client for testing enricher functionality."""
    def __init__(self, payload) -> None:
        self.payload = payload
        self.closed = False
        self.calls: list[tuple[str, dict | None]] = []

    def get_json(
        self, path: str, params=None
    ) -> dict:  # noqa: ANN001 - тестовая заглушка
        """Return payload or raise exception."""
        self.calls.append((path, params))
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload

    def close(self) -> None:
        """Close the dummy client."""
        self.closed = True


class _DummyEnricher(BaseEnricherClient):
    """Dummy enricher client for testing base functionality."""
    
    def __init__(self, payload) -> None:
        self.payload = payload
        self.closed = False
        self.calls: list[tuple[str, dict | None]] = []
        # Initialize BaseEnricherClient with dummy client
        dummy_client = _DummyApiClient(payload)
        super().__init__(dummy_client, source="test")
    
    def fetch(
        self, path: str = "/dummy", params: dict | None = None
    ) -> Iterator[dict]:
        """Fetch data using the base _get method."""
        return self._get(path, params=params)


def test_get_flattens_results_array_into_iterator():
    """Test that _get flattens results array into iterator."""
    payload = {"results": [{"id": 1}, {"id": 2}]}
    client = _DummyEnricher(payload)

    assert list(client.fetch()) == [{"id": 1}, {"id": 2}]
    # Check that the internal api_client was called correctly
    assert client.api_client._api_client.calls == [("/dummy", None)]


def test_get_wraps_single_payloads_and_raw_values():
    """Test that _get wraps single payloads and raw values correctly."""
    mapping_client = _DummyEnricher({"value": 1})
    raw_client = _DummyEnricher("text")

    assert list(mapping_client.fetch()) == [{"value": 1}]
    assert list(raw_client.fetch()) == [{"result": "text"}]


def test_errors_are_normalized_to_request_exception():
    """Test that errors are normalized to RequestException."""
    failing_client = _DummyEnricher(ValueError("boom"))

    with pytest.raises(client_exceptions.RequestException):
        next(failing_client.fetch())


def test_get_closes_client_on_exception():
    """Test that _get closes client on exception."""
    api_client = _DummyApiClient(client_exceptions.HTTPError("boom"))
    client = _DummyEnricher(api_client.payload)

    with pytest.raises(client_exceptions.HTTPError):
        next(client.fetch())

    assert api_client.closed is True


def test_close_delegates_to_api_client():
    """Test that close delegates to API client."""
    api_client = _DummyApiClient({"value": 1})
    client = _DummyEnricher(api_client, "dummy")

    client.close()

    assert api_client.closed is True
