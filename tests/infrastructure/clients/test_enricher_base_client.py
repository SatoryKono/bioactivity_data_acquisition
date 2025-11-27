"""Test functionality of base enricher client."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from bioetl.clients import client_exceptions
from bioetl.clients.enrichers._base import _BaseEnricherClient


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


class _DummyEnricher(_BaseEnricherClient):
    """Dummy enricher client for testing base functionality."""
    def fetch(
        self, path: str = "/dummy", params: dict | None = None
    ) -> Iterator[dict]:
        """Fetch data using the base _get method."""
        return self._get(path, params=params)


def test_get_flattens_results_array_into_iterator():
    """Test that _get flattens results array into iterator."""
    api_client = _DummyApiClient({"results": [{"id": 1}, {"id": 2}]})
    client = _DummyEnricher(api_client, "dummy")

    assert list(client.fetch()) == [{"id": 1}, {"id": 2}]
    assert api_client.calls == [("/dummy", None)]


def test_get_wraps_single_payloads_and_raw_values():
    """Test that _get wraps single payloads and raw values correctly."""
    mapping_client = _DummyEnricher(_DummyApiClient({"value": 1}), "dummy")
    raw_client = _DummyEnricher(_DummyApiClient("text"), "dummy")

    assert list(mapping_client.fetch()) == [{"value": 1}]
    assert list(raw_client.fetch()) == [{"result": "text"}]


def test_errors_are_normalized_to_request_exception():
    """Test that errors are normalized to RequestException."""
    failing_client = _DummyEnricher(
        _DummyApiClient(ValueError("boom")),
        "dummy",
    )

    with pytest.raises(client_exceptions.RequestException):
        next(failing_client.fetch())


def test_get_closes_client_on_exception():
    """Test that _get closes client on exception."""
    api_client = _DummyApiClient(client_exceptions.HTTPError("boom"))
    client = _DummyEnricher(api_client, "dummy")

    with pytest.raises(client_exceptions.HTTPError):
        next(client.fetch())

    assert api_client.closed is True


def test_close_delegates_to_api_client():
    """Test that close delegates to API client."""
    api_client = _DummyApiClient({"value": 1})
    client = _DummyEnricher(api_client, "dummy")

    client.close()

    assert api_client.closed is True
