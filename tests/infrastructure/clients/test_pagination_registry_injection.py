from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, cast
from unittest.mock import MagicMock

from bioetl.clients.chembl import BaseChemblClient, ChemblEntityClient
from bioetl.core.http.pagination import PaginationStrategy

PaginationFactoryMap = dict[str, Callable[[], PaginationStrategy]]


class _DummyPaginationStrategy:
    def iter_pages(
        self, initial_response: Any, transport: Any, **kwargs: Any
    ) -> Iterator[Any]:
        yield initial_response  # pragma: no cover - interface stub


class _IteratorPaginationStrategy:
    def __init__(self, pages: Sequence[Mapping[str, object]]) -> None:
        self.pages = pages
        self.calls: list[Mapping[str, object]] = []

    def iter_pages(
        self,
        initial_response: Any,
        transport: Any,
        **kwargs: Any,
    ) -> Iterator[Mapping[str, object]]:
        self.calls.append({"initial": initial_response, "kwargs": kwargs})
        yield initial_response
        yield from self.pages


def test_chembl_client_uses_registry_strategy_by_name() -> None:
    """Test that ChemblEntityClient uses the strategy from the registry."""
    factories: PaginationFactoryMap = {}
    expected_strategy = cast(PaginationStrategy, _DummyPaginationStrategy())
    factories["dummy"] = lambda: expected_strategy

    transport = MagicMock()

    client = ChemblEntityClient(
        transport,
        "assay",
        pagination_strategy_name="dummy",
        pagination_factories=factories,
    )

    assert client.pagination_strategy is expected_strategy


def test_base_chembl_client_uses_page_param_by_name() -> None:
    """Test that BaseChemblClient uses the page param strategy by name."""
    factories: PaginationFactoryMap = {}
    page_param_strategy = cast(PaginationStrategy, _DummyPaginationStrategy())
    factories["page_param"] = lambda: page_param_strategy

    transport = MagicMock()

    client = BaseChemblClient(
        transport,
        "activity",
        pagination_strategy_name="page_param",
        pagination_factories=factories,
    )

    assert client.pagination_strategy is page_param_strategy


def test_iterate_records_prefers_fetcher_and_respects_ids_pagination() -> None:
    """Test iterate_records with fetcher, IDs, and pagination."""
    transport = MagicMock()
    pages = [
        {"results": [{"id": 2}]},
        {"results": [{"id": 3}]},
    ]
    pagination = cast(PaginationStrategy, _IteratorPaginationStrategy(pages))
    factories: PaginationFactoryMap = {"dummy": lambda: pagination}

    client = ChemblEntityClient(
        transport,
        "assay",
        pagination_strategy_name="dummy",
        pagination_factories=factories,
    )

    fetch_calls: list[Sequence[str] | None] = []

    def fetcher(ids: Sequence[str] | None) -> Any:
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
    # We need to access the specific attributes of our dummy strategy
    # but strictly typed it is PaginationStrategy.
    # We can cast back for the test assertions.
    strategy_impl = cast(_IteratorPaginationStrategy, pagination)
    assert strategy_impl.calls
    assert (
        strategy_impl.calls[0]["kwargs"]["page_key"]
        == "results"
    )
