from __future__ import annotations

from typing import Any, Iterator, Mapping

from bioetl.clients.chembl._base import BaseChemblClient
from bioetl.clients.common import NextLinkPagination, PageParamPagination
from bioetl.clients.entities._base import _BaseEntityClient


class DummyApiClientNextLink:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Mapping[str, Any]]] = []

    def get_json(self, endpoint: str, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        params = params or {}
        self.calls.append((endpoint, dict(params)))
        if endpoint == "/test":
            return {"results": [{"id": 1}], "next": "/test?page=2"}
        if endpoint == "/test?page=2":
            return {"results": [{"id": 2}]}
        return {"detail": "done"}


class DummyApiClientPageParam:
    def __init__(self, pages: list[Mapping[str, Any]]) -> None:
        self.calls: list[dict[str, Any]] = []
        self._pages = pages

    def paginate_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[Mapping[str, Any]]:
        self.calls.append(
            {
                "endpoint": endpoint,
                "params": dict(params or {}),
                "page_key": page_key,
                "next_key": next_key,
                "page_param": page_param,
            }
        )
        yield from self._pages


def test_next_link_pagination_tracks_params_and_next_link() -> None:
    api_client = DummyApiClientNextLink()
    client = BaseChemblClient(api_client, "test", pagination_strategy=NextLinkPagination())

    items = list(client.fetch_all(page_size=50))

    assert items == [{"id": 1}, {"id": 2}]
    assert api_client.calls == [("/test", {"limit": 50}), ("/test?page=2", {})]


def test_next_link_pagination_fallback_without_results() -> None:
    api_client = DummyApiClientNextLink()

    def single_payload(endpoint: str, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        api_client.calls.append((endpoint, dict(params or {})))
        return {"detail": "no_results"}

    api_client.get_json = single_payload  # type: ignore[assignment]

    client = BaseChemblClient(api_client, "test", pagination_strategy=NextLinkPagination())
    items = list(client.fetch_all())

    assert items == [{"detail": "no_results"}]
    assert api_client.calls == [("/test", {"limit": 1000})]


def test_page_param_pagination_uses_paginate_json_and_merges_params() -> None:
    pages = [{"results": [{"id": "a"}]}, {"results": [{"id": "b"}]}]
    api_client = DummyApiClientPageParam(pages)
    client = _BaseEntityClient(api_client, "records", pagination_strategy=PageParamPagination())

    items = list(client.fetch_all(page_size=2, params={"foo": "bar"}))

    assert items == [{"id": "a"}, {"id": "b"}]
    assert api_client.calls == [
        {
            "endpoint": "/records",
            "params": {"limit": 2, "foo": "bar"},
            "page_key": "results",
            "next_key": "next",
            "page_param": "page",
        }
    ]


def test_page_param_pagination_fallback_on_missing_results() -> None:
    pages = [{"detail": "empty_page"}]
    api_client = DummyApiClientPageParam(pages)
    client = _BaseEntityClient(api_client, "records", pagination_strategy=PageParamPagination())

    assert list(client.fetch_all()) == [{"detail": "empty_page"}]
