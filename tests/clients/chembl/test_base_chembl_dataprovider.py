from collections.abc import Mapping
from typing import Any

import pytest

from bioetl.clients import exceptions as client_exceptions
from bioetl.clients.chembl import BaseChemblClient
from bioetl.clients.base import PaginationParams
from bioetl.core.http.pagination import DefaultPaginationStrategy


class DummyTransport:
    def __init__(self, responses: list[Mapping[str, Any]] | None = None) -> None:
        self.responses = list(responses or [])
        self.calls: list[dict[str, Any]] = []
        self.closed = False
        self.pagination_strategy = DefaultPaginationStrategy()
        self.metadata = {"source": "chembl", "api": "dummy"}

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Mapping[str, Any]:
        del headers, json, timeout_sec, max_retries
        self.calls.append({"method": method, "path": path, "params": params})
        if self.responses:
            return self.responses.pop(0)
        return {}

    def close(self) -> None:  # pragma: no cover - trivial
        self.closed = True


def test_fetch_one_normalizes_and_wraps_errors() -> None:
    transport = DummyTransport(responses=[{"chembl_id": "CHEMBL1"}])
    client = BaseChemblClient(transport=transport, entity="molecule")

    result = list(client.fetch_one("CHEMBL1"))

    assert result == [{"chembl_id": "CHEMBL1"}]

    failing_transport = DummyTransport()

    def _raise(*_: Any, **__: Any) -> Mapping[str, Any]:
        raise RuntimeError("boom")

    failing_transport.request = _raise  # type: ignore[assignment]
    failing_client = BaseChemblClient(transport=failing_transport, entity="molecule")

    with pytest.raises(client_exceptions.ProviderError):
        list(failing_client.fetch_one("CHEMBL42"))


def test_iter_pages_respects_pagination_and_normalization() -> None:
    responses = [
        {"results": [{"id": 1}], "next": "/molecule?page=2"},
        {"results": [{"id": 2}], "next": None},
    ]
    transport = DummyTransport(responses=responses)
    client = BaseChemblClient(transport=transport, entity="molecule")

    pages = list(
        client.iter_pages(
            query={"foo": "bar"}, pagination=PaginationParams(page_size=1)
        )
    )

    assert [page.items for page in pages] == [[{"id": 1}], [{"id": 2}]]
    assert pages[0].next_cursor == "/molecule?page=2"
    assert transport.calls[0]["params"] == {"foo": "bar", "limit": 1}


def test_fetch_many_flattens_pages() -> None:
    responses = [
        {"results": [{"id": 1}], "next": "/molecule?page=2"},
        {"results": [{"id": 2}], "next": None},
    ]
    transport = DummyTransport(responses=responses)
    client = BaseChemblClient(transport=transport, entity="molecule")

    records = list(
        client.fetch_many(
            query={"foo": "bar"}, pagination=PaginationParams(page_size=1)
        )
    )

    assert records == [{"id": 1}, {"id": 2}]


def test_metadata_and_close_delegation() -> None:
    transport = DummyTransport(responses=[])
    client = BaseChemblClient(transport=transport, entity="molecule")

    assert client.metadata() == {"source": "chembl", "api": "dummy"}

    client.close()

    assert transport.closed is True
