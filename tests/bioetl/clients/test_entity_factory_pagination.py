from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

from bioetl.clients import NextLinkPagination, PageParamPagination, PaginationStrategy
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.clients.entities import ChemblEntityClientFactory
from bioetl.clients.factories import default_chembl_factory
from bioetl.config import PipelineConfig
from bioetl.infra import PaginationRegistry


class _ScriptedTransport(ApiTransportProtocol):
    def __init__(self, responses: list[Mapping[str, Any]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
    ) -> Mapping[str, Any]:
        del headers, json
        self.calls.append((method, path, dict(params or {})))
        index = len(self.calls) - 1
        if index < len(self.responses):
            return self.responses[index]
        return {"results": []}

    def close(self) -> None:  # pragma: no cover - protocol stub
        return None


class _StaticPagination(PaginationStrategy):
    def __init__(self, extra_page: Mapping[str, Any]) -> None:
        self.extra_page = extra_page
        self.calls: list[Mapping[str, Any]] = []

    def iter_pages(
        self,
        initial_response: Mapping[str, Any],
        transport: Any,
        *,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        logger: Any | None = None,
        page_key: str | None = None,
        next_key: str | None = None,
        page_param: str | None = None,
        normalize: Any | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        del transport, logger, page_key, next_key, page_param, normalize
        self.calls.append({"endpoint": endpoint, "params": dict(params or {})})
        yield initial_response
        yield self.extra_page


def test_entity_factory_passes_explicit_strategy_instance() -> None:
    transport = _ScriptedTransport([{"results": [{"id": 1}]}])
    pagination = _StaticPagination({"results": [{"id": 2}]})

    factory = ChemblEntityClientFactory(
        lambda: transport,
        pagination_strategy=pagination,
    )

    client = factory.assay()

    assert client.pagination_strategy is pagination
    assert list(client.list(page_size=1)) == [{"id": 1}, {"id": 2}]
    assert pagination.calls == [{"endpoint": "/assay", "params": {"limit": 1}}]


def test_default_factory_resolves_named_strategy_for_pagination() -> None:
    registry = PaginationRegistry()
    registry.register("page_param", lambda **_: PageParamPagination())

    transport = _ScriptedTransport(
        [
            {"results": [{"id": 1}]},
            {"results": [{"id": 2}]},
            {"results": []},
        ]
    )

    factory = default_chembl_factory(
        PipelineConfig(),
        transport_factory=lambda: transport,
        pagination_registry=registry,
        pagination_strategy_name="page_param",
    )

    client = factory["activity"]()

    assert list(client.list(page_size=1)) == [{"id": 1}, {"id": 2}]
    assert transport.calls == [
        ("GET", "/activity", {"limit": 1}),
        ("GET", "/activity", {"limit": 1, "page": 2}),
        ("GET", "/activity", {"limit": 1, "page": 3}),
    ]


def test_factory_with_next_link_strategy_follows_links() -> None:
    transport = _ScriptedTransport(
        [
            {"results": [{"id": 1}], "next": "/activity?offset=1"},
            {"results": [{"id": 2}]},
        ]
    )

    factory = ChemblEntityClientFactory(
        lambda: transport,
        pagination_strategy=NextLinkPagination(),
    )

    client = factory.activity()

    assert list(client.list(page_size=1)) == [{"id": 1}, {"id": 2}]
    assert transport.calls == [
        ("GET", "/activity", {"limit": 1}),
        ("GET", "/activity?offset=1", {}),
    ]
