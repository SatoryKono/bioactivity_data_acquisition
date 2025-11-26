from __future__ import annotations

from enum import Enum
from typing import Callable, Type

from bioetl.clients.chembl._base import ChemblEntityClient
from bioetl.clients.common import ApiTransportProtocol, EntityClientProtocol
from bioetl.infra.pagination_registry import PaginationRegistry


class ChemblEntity(str, Enum):
    ACTIVITY = "activity"
    ASSAY = "assay"
    TARGET = "target"
    TESTITEM = "testitem"
    DOCUMENT = "document"


def _build_entity_client(name: str, entity: ChemblEntity) -> Type[ChemblEntityClient]:
    class EntityClient(ChemblEntityClient):
        def __init__(self, transport: ApiTransportProtocol):
            super().__init__(transport, entity)

    EntityClient.__name__ = name
    EntityClient.__qualname__ = name
    return EntityClient


class ChemblEntityClientFactory:
    """Конфигурационный слой для сборки клиентов сущностей ChEMBL."""

    def __init__(
        self,
        transport_factory: Callable[[], ApiTransportProtocol],
        *,
        pagination_registry: PaginationRegistry | None = None,
        pagination_strategy_name: str | None = None,
    ):
        self._transport_factory = transport_factory
        self._pagination_registry = pagination_registry
        self._pagination_strategy_name = pagination_strategy_name

    def create(self, entity: ChemblEntity | str) -> EntityClientProtocol:
        entity_name = ChemblEntity(entity).value if not isinstance(entity, ChemblEntity) else entity.value
        return ChemblEntityClient(
            self._transport_factory(),
            entity_name,
            pagination_registry=self._pagination_registry,
            pagination_strategy_name=self._pagination_strategy_name,
        )

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
