"""Integration tests for enricher clients."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, cast

import pytest

from bioetl.clients import exceptions as client_exceptions
from bioetl.clients.providers import CrossrefClient
from bioetl.core.http.interfaces import BaseApiClient


class FakePagingApiClient:
    """Fake API client for testing pagination."""

    def __init__(
        self,
        *,
        fetch_one_payload: Any = None,
        batch_pages: Iterable[Any] | None = None,
    ) -> None:
        self.fetch_one_payload = fetch_one_payload
        self.batch_pages = list(batch_pages or [])
        self.fetch_one_calls: list[dict[str, Any]] = []
        self.fetch_batch_calls: list[dict[str, Any]] = []
        self.closed = False

    def fetch_one(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Any:
        """Fetch one record."""
        self.fetch_one_calls.append(
            {
                "endpoint": endpoint,
                "params": params,
                "headers": headers,
                "timeout_sec": timeout_sec,
                "max_retries": max_retries,
            }
        )
        return self.fetch_one_payload

    def fetch_batch(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        page_key: str = "results",
        next_key: str | None = "next",
        page_param: str | None = "page",
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Iterable[Any]:
        """Fetch batch of records."""
        self.fetch_batch_calls.append(
            {
                "endpoint": endpoint,
                "params": params,
                "headers": headers,
                "page_key": page_key,
                "next_key": next_key,
                "page_param": page_param,
                "timeout_sec": timeout_sec,
                "max_retries": max_retries,
            }
        )
        return iter(self.batch_pages)

    def close(self) -> None:  # pragma: no cover - simple flag setter
        """Close the client."""
        self.closed = True


class FailingBatchApiClient(FakePagingApiClient):
    """API client that fails during batch fetch."""

    def fetch_batch(
        self, *_args: Any, **_kwargs: Any
    ) -> Iterable[Any]:  # noqa: ANN001, ARG001 - test stub
        """Fetch batch with simulated failure."""
        def _generator() -> Iterable[Any]:
            yield {"results": [{"id": 1}]}
            raise client_exceptions.RequestException("boom")

        return _generator()


@pytest.mark.parametrize(
    "payload, page_key",
    [({"custom": []}, "custom"), ({"results": []}, None)],
)
def test_route_provider_fetch_one_yields_fallback_when_page_empty(
    payload: Mapping[str, Any], page_key: str | None
) -> None:
    """Test route provider fetch one yields fallback when page empty."""
    api_client = FakePagingApiClient(fetch_one_payload=payload)
    client = CrossrefClient(cast(BaseApiClient, api_client))

    result = list(client.fetch_one("10.1000/xyz", page_key=page_key))

    assert result == [{"result": payload}]
    assert api_client.fetch_one_calls == [
        {
            "endpoint": "/works/10.1000/xyz",
            "params": None,
            "headers": None,
            "timeout_sec": None,
            "max_retries": None,
        }
    ]


def test_route_provider_fetch_batch_paginates_and_passes_params() -> None:
    """Test route provider fetch batch paginates and passes params."""
    pages = [
        {"results": [{"id": 1}]},
        {"results": [{"id": 2}]},
    ]
    api_client = FakePagingApiClient(batch_pages=pages)
    client = CrossrefClient(cast(BaseApiClient, api_client))

    records = list(
        client.fetch_batch("aspirin", params={"filter": "type:journal"})
    )

    assert records == [{"id": 1}, {"id": 2}]
    assert api_client.fetch_batch_calls == [
        {
            "endpoint": "/works",
            "params": {"query": "aspirin", "filter": "type:journal"},
            "headers": None,
            "page_key": "results",
            "next_key": "next",
            "page_param": "page",
            "timeout_sec": None,
            "max_retries": None,
        }
    ]


def test_route_provider_closes_transport_on_iteration_error() -> None:
    """Test route provider closes transport on iteration error."""
    api_client = FailingBatchApiClient()
    client = CrossrefClient(cast(BaseApiClient, api_client))

    with pytest.raises(client_exceptions.RequestException):
        list(client.fetch_batch("broken"))

    assert api_client.closed is True
