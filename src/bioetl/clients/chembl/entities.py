"""Base classes and factories for ChEMBL entity clients."""
from __future__ import annotations

from collections.abc import Mapping
from enum import Enum
from typing import Callable, Type

from bioetl.clients.chembl.base import ChemblEntityClient
from bioetl.clients.chembl.pagination import PaginationFactory
from bioetl.core.http.api_entity_client import EntityClientProtocol
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import PaginationStrategy


class ChemblEntity(str, Enum):
    """Enumeration of supported ChEMBL entities."""

    ACTIVITY = "activity"
    ASSAY = "assay"
    TARGET = "target"
    TESTITEM = "testitem"
    DOCUMENT = "document"


CHEMBL_ALLOWED_ENTITIES: tuple[str, ...] = tuple(
    member.value for member in ChemblEntity
)


def _build_entity_client(
    name: str, entity: ChemblEntity
) -> Type[ChemblEntityClient]:
    class EntityClient(ChemblEntityClient):
        """Dynamically generated entity client."""

        def __init__(self, transport: ApiTransportProtocol, **kwargs):
            super().__init__(transport, entity, **kwargs)

    EntityClient.__name__ = name
    EntityClient.__qualname__ = name
    return EntityClient


class ChemblEntityClientFactory:
    """Factory for creating configured ChEMBL entity clients.

    Handles dependency injection for transport, pagination, and specific
    entity configuration.
    """

    def __init__(
        self,
        transport_factory: Callable[[], ApiTransportProtocol],
        *,
        pagination_strategy_name: str | None = None,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_factories: Mapping[str, PaginationFactory] | None = None,
    ):
        self._transport_factory = transport_factory
        self._pagination_strategy_name = pagination_strategy_name
        self._pagination_strategy = pagination_strategy
        self._pagination_factories = pagination_factories

    def create(self, entity: ChemblEntity | str) -> EntityClientProtocol:
        """Create a client for the specified entity."""
        entity_name = (
            ChemblEntity(entity).value
            if not isinstance(entity, ChemblEntity)
            else entity.value
        )
        return ChemblEntityClient(
            self._transport_factory(),
            entity_name,
            pagination_strategy=self._pagination_strategy,
            pagination_strategy_name=self._pagination_strategy_name,
            pagination_factories=self._pagination_factories,
        )

    def _create_specific(
        self, client_cls: Type[ChemblEntityClient]
    ) -> EntityClientProtocol:
        # client_cls is a dynamic subclass that binds the 'entity' argument
        # in its __init__, so we don't need to pass it here.
        return client_cls(  # type: ignore[call-arg]
            self._transport_factory(),
            pagination_strategy=self._pagination_strategy,
            pagination_strategy_name=self._pagination_strategy_name,
            pagination_factories=self._pagination_factories,
        )

    def activity(self) -> EntityClientProtocol:
        """Create an activity client."""
        return self._create_specific(ChemblActivityClient)

    def assay(self) -> EntityClientProtocol:
        """Create an assay client."""
        return self._create_specific(ChemblAssayClient)

    def target(self) -> EntityClientProtocol:
        """Create a target client."""
        return self._create_specific(ChemblTargetClient)

    def testitem(self) -> EntityClientProtocol:
        """Create a testitem client."""
        return self._create_specific(ChemblTestItemClient)

    def document(self) -> EntityClientProtocol:
        """Create a document client."""
        return self._create_specific(ChemblDocumentClient)


ChemblActivityClient = _build_entity_client(
    "ChemblActivityClient",
    ChemblEntity.ACTIVITY,
)
ChemblAssayClient = _build_entity_client(
    "ChemblAssayClient",
    ChemblEntity.ASSAY,
)
ChemblTargetClient = _build_entity_client(
    "ChemblTargetClient",
    ChemblEntity.TARGET,
)
ChemblTestItemClient = _build_entity_client(
    "ChemblTestItemClient",
    ChemblEntity.TESTITEM,
)
ChemblDocumentClient = _build_entity_client(
    "ChemblDocumentClient",
    ChemblEntity.DOCUMENT,
)

__all__ = [
    "ChemblEntity",
    "ChemblEntityClient",
    "ChemblEntityClientFactory",
    "CHEMBL_ALLOWED_ENTITIES",
    "ChemblActivityClient",
    "ChemblAssayClient",
    "ChemblTargetClient",
    "ChemblTestItemClient",
    "ChemblDocumentClient",
]
