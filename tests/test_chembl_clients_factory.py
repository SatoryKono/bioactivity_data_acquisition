from __future__ import annotations

from bioetl.clients.entities import (
    ChemblActivityClient,
    ChemblAssayClient,
    ChemblDocumentClient,
    ChemblEntity,
    ChemblEntityClientFactory,
    ChemblTargetClient,
    ChemblTestItemClient,
)


class _DummyAPIClient:
    def get(self, *_args, **_kwargs):  # pragma: no cover - not exercised in this test
        raise NotImplementedError


def test_entity_client_factory_creates_specialized_clients():
    api_client = _DummyAPIClient()

    clients = {
        "activity": ChemblEntityClientFactory.activity(api_client),
        "assay": ChemblEntityClientFactory.assay(api_client),
        "target": ChemblEntityClientFactory.target(api_client),
        "testitem": ChemblEntityClientFactory.testitem(api_client),
        "document": ChemblEntityClientFactory.document(api_client),
    }

    assert {name: client.entity for name, client in clients.items()} == {
        "activity": "activity",
        "assay": "assay",
        "target": "target",
        "testitem": "testitem",
        "document": "document",
    }


def test_entity_specific_aliases_preserve_entity_names():
    api_client = _DummyAPIClient()

    assert ChemblActivityClient(api_client).entity == ChemblEntity.ACTIVITY.value
    assert ChemblAssayClient(api_client).entity == ChemblEntity.ASSAY.value
    assert ChemblTargetClient(api_client).entity == ChemblEntity.TARGET.value
    assert ChemblTestItemClient(api_client).entity == ChemblEntity.TESTITEM.value
    assert ChemblDocumentClient(api_client).entity == ChemblEntity.DOCUMENT.value
