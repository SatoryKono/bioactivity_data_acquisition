from __future__ import annotations

from enum import Enum
from typing import Type

from bioetl.clients.entities._base import _BaseEntityClient
from bioetl.core.http.api_client import UnifiedAPIClient


class ChemblEntity(str, Enum):
    ACTIVITY = "activity"
    ASSAY = "assay"
    TARGET = "target"
    TESTITEM = "testitem"
    DOCUMENT = "document"


class ChemblEntityClient(_BaseEntityClient):
    def __init__(self, api_client: UnifiedAPIClient, entity: ChemblEntity | str) -> None:
        entity_name = ChemblEntity(entity).value if not isinstance(entity, ChemblEntity) else entity.value
        super().__init__(api_client, entity_name)


def _build_entity_client(name: str, entity: ChemblEntity) -> Type[ChemblEntityClient]:
    class EntityClient(ChemblEntityClient):
        def __init__(self, api_client: UnifiedAPIClient):
            super().__init__(api_client, entity)

    EntityClient.__name__ = name
    EntityClient.__qualname__ = name
    return EntityClient


class ChemblEntityClientFactory:
    """Factory helpers for building typed ChEMBL entity clients."""

    @staticmethod
    def create(entity: ChemblEntity | str, api_client: UnifiedAPIClient) -> ChemblEntityClient:
        return ChemblEntityClient(api_client, entity)

    @staticmethod
    def activity(api_client: UnifiedAPIClient) -> ChemblEntityClient:
        return ChemblEntityClientFactory.create(ChemblEntity.ACTIVITY, api_client)

    @staticmethod
    def assay(api_client: UnifiedAPIClient) -> ChemblEntityClient:
        return ChemblEntityClientFactory.create(ChemblEntity.ASSAY, api_client)

    @staticmethod
    def target(api_client: UnifiedAPIClient) -> ChemblEntityClient:
        return ChemblEntityClientFactory.create(ChemblEntity.TARGET, api_client)

    @staticmethod
    def testitem(api_client: UnifiedAPIClient) -> ChemblEntityClient:
        return ChemblEntityClientFactory.create(ChemblEntity.TESTITEM, api_client)

    @staticmethod
    def document(api_client: UnifiedAPIClient) -> ChemblEntityClient:
        return ChemblEntityClientFactory.create(ChemblEntity.DOCUMENT, api_client)


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
