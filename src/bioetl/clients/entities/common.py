from __future__ import annotations

from enum import Enum
from typing import Callable, Type

from bioetl.base_classes import BaseApiClient
from bioetl.clients.chembl._base import ChemblEntityClient
from bioetl.clients.common import EntityClientProtocol


class ChemblEntity(str, Enum):
    ACTIVITY = "activity"
    ASSAY = "assay"
    TARGET = "target"
    TESTITEM = "testitem"
    DOCUMENT = "document"


def _build_entity_client(name: str, entity: ChemblEntity) -> Type[ChemblEntityClient]:
    class EntityClient(ChemblEntityClient):
        def __init__(self, transport: BaseApiClient):
            super().__init__(transport, entity)

    EntityClient.__name__ = name
    EntityClient.__qualname__ = name
    return EntityClient


class ChemblEntityClientFactory:
    """Конфигурационный слой для сборки клиентов сущностей ChEMBL."""

    def __init__(self, transport_factory: Callable[[], BaseApiClient]):
        self._transport_factory = transport_factory

    def create(self, entity: ChemblEntity | str) -> EntityClientProtocol:
        entity_name = ChemblEntity(entity).value if not isinstance(entity, ChemblEntity) else entity.value
        return ChemblEntityClient(self._transport_factory(), entity_name)

    def activity(self) -> EntityClientProtocol:
        return self.create(ChemblEntity.ACTIVITY)

    def assay(self) -> EntityClientProtocol:
        return self.create(ChemblEntity.ASSAY)

    def target(self) -> EntityClientProtocol:
        return self.create(ChemblEntity.TARGET)

    def testitem(self) -> EntityClientProtocol:
        return self.create(ChemblEntity.TESTITEM)

    def document(self) -> EntityClientProtocol:
        return self.create(ChemblEntity.DOCUMENT)


ChemblActivityClient = _build_entity_client("ChemblActivityClient", ChemblEntity.ACTIVITY)
ChemblAssayClient = _build_entity_client("ChemblAssayClient", ChemblEntity.ASSAY)
ChemblTargetClient = _build_entity_client("ChemblTargetClient", ChemblEntity.TARGET)
ChemblTestItemClient = _build_entity_client("ChemblTestItemClient", ChemblEntity.TESTITEM)
ChemblDocumentClient = _build_entity_client("ChemblDocumentClient", ChemblEntity.DOCUMENT)

__all__ = [
    "ChemblEntity",
    "ChemblEntityClient",
    "ChemblEntityClientFactory",
    "ChemblActivityClient",
    "ChemblAssayClient",
    "ChemblTargetClient",
    "ChemblTestItemClient",
    "ChemblDocumentClient",
]
