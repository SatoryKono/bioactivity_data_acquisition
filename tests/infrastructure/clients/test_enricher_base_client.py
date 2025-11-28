"""Test functionality of base enricher client."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, cast

import pytest

from bioetl.clients import exceptions as client_exceptions
from bioetl.clients.enrichers.base import BaseEnricherClient
from bioetl.core.http.interfaces import BaseApiClient


class _DummyApiClient:
    """Dummy API client for testing enricher functionality."""

    def __init__(self, payload: Any) -> None:
        self.payload = payload
        self.closed = False
        self.calls: list[tuple[str, dict[str, Any] | None]] = []

    def get_json(
        self, path: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Return payload or raise exception."""
        self.calls.append((path, params))
        if isinstance(self.payload, Exception):
            raise self.payload
        return cast("dict[str, Any]", self.payload)

    def paginate_json(
        self,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Dummy pagination."""
        raise NotImplementedError

    def iterate_records(
        self,
        *,
        ids: Any | None = None,
        page_size: int | None = None,
        fetcher: Any | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Dummy iterate_records."""
        raise NotImplementedError

    def fetch_one(
        self,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> dict[str, Any]:
        """Return payload or raise exception."""
        del headers, timeout_sec, max_retries
        self.calls.append((endpoint, params))
        if isinstance(self.payload, Exception):
            raise self.payload
        return cast("dict[str, Any]", self.payload)

    def fetch_batch(
        self,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Dummy fetch_batch."""
        raise NotImplementedError

    def close(self) -> None:
        """Close the dummy client."""
        self.closed = True

    @property
    def pagination_strategy(self) -> None:
        return None

    @property
    def default_timeout_sec(self) -> float | None:
        return None

    @property
    def default_max_retries(self) -> int | None:
        return None


class _DummyEnricher(BaseEnricherClient):
    """Dummy enricher client for testing base functionality."""

    def __init__(self, payload: Any, source: str = "test") -> None:
        self.payload = payload
        self.closed = False
        self.calls: list[tuple[str, dict[str, Any] | None]] = []
        # Initialize BaseEnricherClient with dummy client
        if isinstance(payload, _DummyApiClient):
            dummy_client = payload
        else:
            dummy_client = _DummyApiClient(payload)
        super().__init__(cast(BaseApiClient, dummy_client), source=source)

    def fetch(
        self, path: str = "/dummy", params: dict[str, Any] | None = None
    ) -> Iterator[dict[str, Any]]:
        """Fetch data using the base fetch_one method."""
        return cast(
            Iterator[dict[str, Any]], self.fetch_one(path, params=params)
        )


def test_get_flattens_results_array_into_iterator() -> None:
    """Test that fetch_one flattens results array into iterator."""
    payload = {"results": [{"id": 1}, {"id": 2}]}
    client = _DummyEnricher(payload)

    assert list(client.fetch()) == [{"id": 1}, {"id": 2}]
    # Check that the internal api_client was called correctly
    dummy = cast(_DummyApiClient, client.api_client)
    assert dummy.calls == [("/dummy", None)]


def test_get_wraps_single_payloads_and_raw_values() -> None:
    """Test that fetch_one wraps single payloads and raw values correctly."""
    mapping_client = _DummyEnricher({"value": 1})
    raw_client = _DummyEnricher("text")

    assert list(mapping_client.fetch()) == [{"value": 1}]
    assert list(raw_client.fetch()) == [{"result": "text"}]


def test_errors_are_normalized_to_request_exception() -> None:
    """Test that errors are normalized to RequestException."""
    failing_client = _DummyEnricher(ValueError("boom"))

    with pytest.raises(client_exceptions.RequestException):
        next(failing_client.fetch())


def test_get_closes_client_on_exception() -> None:
    """Test that fetch_one closes client on exception."""
    api_client = _DummyApiClient(client_exceptions.HTTPError("boom"))
    client = _DummyEnricher(api_client)

    with pytest.raises(client_exceptions.HTTPError):
        next(client.fetch())

    assert api_client.closed is True


def test_close_delegates_to_api_client() -> None:
    """Test that close delegates to API client."""
    api_client = _DummyApiClient({"value": 1})
    client = _DummyEnricher(api_client, "dummy")

    client.close()

    assert api_client.closed is True
