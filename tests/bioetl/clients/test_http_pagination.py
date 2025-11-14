"""Unit tests for shared HTTP pagination primitives."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Iterator

import pytest

from bioetl.clients.http import Paginator, RetryingSession
from bioetl.core.http.api_client import CircuitBreakerOpenError


class _StubResponse:
    def __init__(self, payload: Mapping[str, Any], *, url: str) -> None:
        self._payload = payload
        self.status_code = 200
        self.url = url

    def json(self) -> Mapping[str, Any]:
        return self._payload


class _StubClient:
    def __init__(self, payloads: Iterator[Mapping[str, Any]]) -> None:
        self._payloads = iter(payloads)
        self.base_url = "https://example.org/chembl/api/data"
        self.name = "stub-client"
        self.calls: list[tuple[str, Mapping[str, Any] | None]] = []

    def get(self, endpoint: str, *, params: Mapping[str, Any] | None = None) -> _StubResponse:
        self.calls.append((endpoint, params))
        payload = next(self._payloads)
        url = endpoint if endpoint.startswith("http") else f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        return _StubResponse(payload, url=url)


class _FailingClient(_StubClient):
    def __init__(self) -> None:
        super().__init__(payloads=iter(()))

    def get(self, endpoint: str, *, params: Mapping[str, Any] | None = None) -> _StubResponse:
        raise CircuitBreakerOpenError("circuit open")


@pytest.mark.unit
def test_paginator_iterates_multiple_pages() -> None:
    """Ensure paginator follows next links and preserves parameters."""

    payloads = iter([
        {
            "page_meta": {"next": "https://example.org/chembl/api/data/activity.json?offset=2"},
            "activities": [{"id": 1}, {"id": 2}],
        },
        {
            "page_meta": {"next": None},
            "activities": [{"id": 3}],
        },
    ])
    client = _StubClient(payloads)
    session = RetryingSession(client)
    paginator = Paginator(session)

    pages = list(
        paginator.iterate_pages(
            "/activity.json",
            params={"select": "value"},
            page_size=2,
            items_key="activities",
        )
    )

    assert len(pages) == 2
    assert [item["id"] for page in pages for item in page.items] == [1, 2, 3]
    assert client.calls[0][0] == "/activity.json"
    assert client.calls[0][1]["limit"] == 2
    assert client.calls[1][0] == "/activity.json?offset=2"
    assert client.calls[1][1] is None


@pytest.mark.unit
def test_paginator_respects_limit() -> None:
    """Ensure paginator stops after reaching the requested limit."""

    payloads = iter([
        {"page_meta": {"next": "/activity.json?offset=1"}, "activities": [{"id": 1}, {"id": 2}]},
        {"page_meta": {"next": None}, "activities": [{"id": 3}]},
    ])
    client = _StubClient(payloads)
    session = RetryingSession(client)
    paginator = Paginator(session)

    pages = list(paginator.iterate_pages("/activity.json", limit=2, items_key="activities"))

    assert len(pages) == 1
    assert [item["id"] for page in pages for item in page.items] == [1, 2]
    assert len(client.calls) == 1


@pytest.mark.unit
def test_retrying_session_propagates_circuit_breaker() -> None:
    """RetryingSession must not swallow CircuitBreakerOpenError."""

    client = _FailingClient()
    session = RetryingSession(client)

    with pytest.raises(CircuitBreakerOpenError):
        session.get_payload("/activity.json")

