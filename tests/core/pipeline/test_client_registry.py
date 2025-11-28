from __future__ import annotations

import pytest

from bioetl.clients.chembl.entities import ChemblEntity
from bioetl.clients.enrichers.factory import EnricherEntity
from bioetl.core.pipeline.types import (
    ClientNamespace,
    ClientRegistry,
    ClientRegistryContext,
)


class DummyFactory:
    def __init__(self) -> None:
        self.calls: list[object] = []

    def create(self, entity: object):  # pragma: no cover - trivial passthrough
        self.calls.append(entity)
        return {"entity": entity}


@pytest.mark.parametrize(
    "namespace,entity",
    [
        (ClientNamespace.CHEMBL, ChemblEntity.ASSAY),
        (ClientNamespace.ENRICHER, EnricherEntity.PUBMED),
    ],
)
def test_client_registry_validates_namespace_and_entity(namespace, entity):
    factory = DummyFactory()
    registry = ClientRegistry({namespace.value: factory})

    client = registry.get(namespace, entity)

    assert client == {"entity": entity}
    assert factory.calls == [entity]


def test_client_registry_context_supports_legacy_strings():
    factory = DummyFactory()
    registry = ClientRegistry({ClientNamespace.ENRICHER.value: factory})
    context = ClientRegistryContext(registry)

    client = context.get_client("enricher:semantic_scholar")

    assert client == {"entity": EnricherEntity.SEMANTIC_SCHOLAR}
    assert factory.calls == [EnricherEntity.SEMANTIC_SCHOLAR]


def test_client_registry_raises_for_unknown_entity():
    registry = ClientRegistry({ClientNamespace.CHEMBL.value: DummyFactory()})

    with pytest.raises(KeyError):
        registry.get(ClientNamespace.CHEMBL, "unknown")
