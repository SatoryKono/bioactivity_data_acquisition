from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from bioetl.clients.entities import (
    ChemblActivityClient,
    ChemblAssayClient,
    ChemblDocumentClient,
    ChemblEntity,
    ChemblEntityClientFactory,
    ChemblTargetClient,
    ChemblTestItemClient,
)
from bioetl.core.http.interfaces import ApiTransportProtocol


class _DummyTransport(ApiTransportProtocol):
    def __init__(self) -> None:
        self.request_called = False

    def request(
        self,
        method: str,
        path: str,
        *,
        headers=None,
        params=None,
        json=None,
    ):  # type: ignore[override]
        self.request_called = True
        raise AssertionError(
            "Transport should not be used during client construction",
        )

    def close(self) -> None:  # pragma: no cover - noop
        return None


def test_entity_client_factory_creates_specialized_clients_without_io():
    transport = _DummyTransport()
    factory = ChemblEntityClientFactory(lambda: transport)

    clients = {
        "activity": factory.activity(),
        "assay": factory.assay(),
        "target": factory.target(),
        "testitem": factory.testitem(),
        "document": factory.document(),
    }

    assert {name: client.entity for name, client in clients.items()} == {
        "activity": "activity",
        "assay": "assay",
        "target": "target",
        "testitem": "testitem",
        "document": "document",
    }
    assert transport.request_called is False


@pytest.mark.parametrize(
    ("builder", "expected_entity"),
    [
        (ChemblActivityClient, ChemblEntity.ACTIVITY),
        (ChemblAssayClient, ChemblEntity.ASSAY),
        (ChemblTargetClient, ChemblEntity.TARGET),
        (ChemblTestItemClient, ChemblEntity.TESTITEM),
        (ChemblDocumentClient, ChemblEntity.DOCUMENT),
    ],
)
def test_entity_specific_aliases_preserve_entity_names(
    builder,
    expected_entity,
) -> None:
    transport = MagicMock(spec=ApiTransportProtocol)

    assert builder(transport).entity == expected_entity.value


def test_factory_uses_transport_factory_each_time():
    transport_factory = MagicMock()
    first = MagicMock(spec=ApiTransportProtocol)
    second = MagicMock(spec=ApiTransportProtocol)
    transport_factory.side_effect = [first, second]

    factory = ChemblEntityClientFactory(transport_factory)

    assert factory.activity().transport is first
    assert factory.assay().transport is second
    assert transport_factory.call_count == 2
