from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from bioetl.clients.base import Page, PaginationParams
from bioetl.clients.base import exceptions as provider_exceptions
from bioetl.clients.chembl.base import BaseChemblClient
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import DefaultPaginationStrategy


class DummyTransport(ApiTransportProtocol):
    def __init__(self, responses: list[Any], metadata: Mapping[str, Any] | None = None):
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []
        self.metadata = dict(metadata or {})
        self.closed = False

    def request(self, method: str, path: str, *, params: Mapping[str, Any] | None = None, **_: Any) -> Any:  # noqa: D401,ARG002
        self.calls.append({"method": method, "path": path, "params": params})
        if not self.responses:
            raise RuntimeError("No responses configured")
        next_response = self.responses.pop(0)
        if isinstance(next_response, Exception):
            raise next_response
        return next_response

    def close(self) -> None:  # pragma: no cover - simple flag for assertions
        self.closed = True


def _make_client(responses: list[Any], metadata: Mapping[str, Any] | None = None) -> tuple[BaseChemblClient, DummyTransport]:
    transport = DummyTransport(responses, metadata=metadata)
    client = BaseChemblClient(
        transport,
        "molecule",
        pagination_strategy=DefaultPaginationStrategy(),
    )
    return client, transport


def test_fetch_one_normalizes_payload() -> None:
    client, transport = _make_client([{"foo": "bar"}])

    records = list(client.fetch_one("123"))

    assert records == [{"foo": "bar"}]
    assert transport.calls[0]["path"] == "/molecule/123"


def test_fetch_one_wraps_provider_errors() -> None:
    client, _ = _make_client([ValueError("boom")])

    with pytest.raises(provider_exceptions.ProviderError):
        list(client.fetch_one("err"))


def test_iter_pages_respects_pagination_params() -> None:
    client, transport = _make_client(
        [
            {"results": [{"id": 1}]},
            {"results": [{"id": 2}], "next": None},
        ]
    )

    pages = list(
        client.iter_pages(
            query={"q": "x"}, pagination=PaginationParams(page_size=1)
        )
    )

    assert [page.items for page in pages] == [[{"id": 1}], [{"id": 2}]]
    assert all(isinstance(page, Page) for page in pages)
    assert transport.calls[0]["params"] == {"q": "x", "limit": 1}
    # Second call should include incremented page param
    assert transport.calls[1]["params"] == {"q": "x", "limit": 1, "page": 2}


def test_fetch_many_flattens_pages() -> None:
    client, _ = _make_client(
        [
            {"results": [{"id": 1}]},
            {"results": [{"id": 2}], "next": None},
        ]
    )

    records = list(client.fetch_many(page_size=1))

    assert records == [{"id": 1}, {"id": 2}]


def test_metadata_includes_transport_info() -> None:
    client, _ = _make_client([], metadata={"release": "v1"})

    meta = client.metadata()

    assert meta["source"] == "chembl"
    assert meta["entity"] == "molecule"
    assert meta["transport"] == {"release": "v1"}


def test_close_closes_transport() -> None:
    client, transport = _make_client([])

    client.close()

    assert transport.closed is True
