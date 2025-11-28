from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from bioetl.clients.chembl.adapter import ChemblTransportAdapter
from bioetl.clients.chembl.entities import (
    ChemblActivityClient,
    ChemblAssayClient,
    ChemblDocumentClient,
    ChemblEntity,
    ChemblEntityClientFactory,
    ChemblEntityClientFactoryConfig,
    ChemblTargetClient,
    ChemblTestItemClient,
)
from bioetl.clients.chembl import BaseChemblClient
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


class _MetadataTransport(ApiTransportProtocol):
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def request(
        self,
        method: str,
        path: str,
        *,
        headers=None,
        params=None,
        json=None,
    ):  # type: ignore[override]
        self.calls.append((method, path))
        return {"page_meta": {"total": 2, "page": 1}, "results": []}

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

    assert factory.activity().transport.base_transport is first
    assert factory.assay().transport.base_transport is second
    assert transport_factory.call_count == 2


def test_base_chembl_client_collects_metadata():
    adapter = ChemblTransportAdapter(_MetadataTransport())
    client = BaseChemblClient(
        adapter,
        "activity",
        pagination_strategy=adapter.pagination_strategy,
    )

    response = client.status()

    assert response["page_meta"] == {"total": 2, "page": 1}
    assert client.metadata == {"total": 2, "page": 1}
    assert adapter.base_transport.calls == [("GET", "/status")]


def test_factory_accepts_config_and_supports_mapping_access():
    transport = _DummyTransport()
    config = ChemblEntityClientFactoryConfig(lambda: transport)

    factory = ChemblEntityClientFactory(config)

    assert factory.config is config
    assert factory["document"]().entity == "document"
