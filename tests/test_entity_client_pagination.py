from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any, Callable

import pytest

from bioetl.clients import client_exceptions
from bioetl.clients.chembl._base import BaseChemblClient
from bioetl.clients.entities._base import _BaseEntityClient


class _DummyApiClient:
    def __init__(self, *, fail_on_get: Exception | None = None) -> None:
        self.fail_on_get = fail_on_get
        self.calls: list[tuple[str, Any, Any, Any, Any, Any]] = []
        self.closed = False

    def get_json(self, endpoint: str, *, params: Mapping[str, Any] | None = None, headers: Mapping[str, str] | None = None):
        self.calls.append(("get", endpoint, params, headers, None, None))
        if self.fail_on_get:
            raise self.fail_on_get
        return {"id": endpoint}

    def paginate_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[Mapping[str, Any]]:
        self.calls.append(("paginate", endpoint, params, page_key, next_key, page_param))
        yield {page_key: [{"payload": endpoint}], next_key: None}

    def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize(
    "client_factory",
    [
        lambda api: _BaseEntityClient(api, "activity"),
        lambda api: BaseChemblClient(api, "activity"),
    ],
)
def test_fetch_all_uses_consistent_pagination(client_factory: Callable[[Any], Any]):
    api = _DummyApiClient()
    client = client_factory(api)

    items = list(client.fetch_all())

    assert items == [{"payload": "/activity"}]
    assert api.calls == [("paginate", "/activity", {"limit": 1000}, "results", "next", "page")]


def test_fetch_all_propagates_pagination_overrides():
    api = _DummyApiClient()
    client = BaseChemblClient(api, "assay")

    items = list(
        client.fetch_all(page_size=10, params={"foo": "bar"}, page_key="items", next_key="next_page", page_param=None)
    )

    assert items == [{"payload": "/assay"}]
    assert api.calls == [("paginate", "/assay", {"limit": 10, "foo": "bar"}, "items", "next_page", None)]


@pytest.mark.parametrize(
    "client_factory",
    [
        lambda api: _BaseEntityClient(api, "target"),
        lambda api: BaseChemblClient(api, "target"),
    ],
)
def test_fetch_by_ids_wraps_non_http_errors(client_factory: Callable[[Any], Any]):
    api = _DummyApiClient(fail_on_get=ValueError("boom"))
    client = client_factory(api)

    with pytest.raises(client_exceptions.RequestException):
        list(client.fetch_by_ids(["1"]))


def test_close_delegates_to_api_client():
    api = _DummyApiClient()
    client = _BaseEntityClient(api, "document")

    client.close()

    assert api.closed
