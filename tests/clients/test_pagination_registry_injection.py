from __future__ import annotations

from unittest.mock import MagicMock

from bioetl.clients.entities import ChemblEntityClient
from bioetl.infra import PaginationRegistry


class _DummyPaginationStrategy:
    def iter_pages(self, initial_response, transport, **kwargs):  # pragma: no cover - interface stub
        yield initial_response


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
