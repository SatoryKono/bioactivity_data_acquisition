from __future__ import annotations

from collections.abc import Iterator, Mapping
from unittest.mock import MagicMock

from bioetl.infrastructure.chembl import BaseChemblClient
from bioetl.clients.entities import ChemblEntityClient
from bioetl.infra import PaginationRegistry


class _DummyPaginationStrategy:
    def iter_pages(self, initial_response, transport, **kwargs):
        yield initial_response  # pragma: no cover - interface stub


class _IteratorPaginationStrategy:
    def __init__(self, pages: list[Mapping[str, object]]) -> None:
        self.pages = pages
        self.calls: list[Mapping[str, object]] = []

    def iter_pages(
        self,
        initial_response,
        transport,
        **kwargs,
    ) -> Iterator[Mapping[str, object]]:  # type: ignore[override]
        self.calls.append({"initial": initial_response, "kwargs": kwargs})
        yield initial_response
        yield from self.pages


def test_chembl_client_uses_registry_strategy_by_name():
    registry = PaginationRegistry()
    expected_strategy = _DummyPaginationStrategy()
    registry.register("dummy", lambda **_: expected_strategy)

    transport = MagicMock()

    client = ChemblEntityClient(
        transport,
        "assay",
        pagination_strategy_name="dummy",
        pagination_registry=registry,
    )

    assert client.pagination_strategy is expected_strategy


def test_base_chembl_client_uses_page_param_by_name():
    registry = PaginationRegistry()
    page_param_strategy = _DummyPaginationStrategy()
    registry.register("page_param", lambda **_: page_param_strategy)

    transport = MagicMock()

    client = BaseChemblClient(
        transport,
        pagination_registry=registry,
        pagination_strategy_name="page_param",
    )

    assert client.pagination_strategy is page_param_strategy


def test_iterate_records_prefers_fetcher_and_respects_ids_and_pagination():
    transport = MagicMock()
    pages = [
        {"results": [{"id": 2}]},
        {"results": [{"id": 3}]},
    ]
    pagination = _IteratorPaginationStrategy(pages)
    registry = PaginationRegistry()
    registry.register("dummy", lambda **_: pagination)

    client = ChemblEntityClient(
        transport,
        "assay",
        pagination_strategy_name="dummy",
        pagination_registry=registry,
    )

    fetch_calls: list[list[str] | None] = []

    def fetcher(ids: list[str] | None):
        fetch_calls.append(ids)
        if ids:
            return [{"id": f"fetched-{ids[0]}"}]
        return None

    records_with_ids = list(
        client.iterate_records(ids=["10"], fetcher=fetcher)
    )
    assert records_with_ids == [{"id": "fetched-10"}]

    transport.request.return_value = {"results": [{"id": 1}]}
    records_paginated = list(
        client.iterate_records(fetcher=fetcher, page_size=1)
    )

    assert records_paginated == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert fetch_calls == [["10"], None]
    assert (
        transport.request.call_args_list[0].args[1]
        == "/assay"
    )
    assert pagination.calls
    assert (
        pagination.calls[0]["kwargs"]["page_key"]
        == "results"
    )
