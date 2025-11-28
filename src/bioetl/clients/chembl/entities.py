"""Base classes and factories for ChEMBL entity clients."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Protocol, Type

from bioetl.clients.chembl.base import (
    BaseChemblEntityProtocol,
    ChemblEntityClient,
)
from bioetl.clients.chembl.pagination import PaginationFactory
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


class ChemblEntityClientProtocol(BaseChemblEntityProtocol, Protocol):
    """Protocol alias for configured ChEMBL entity clients."""


@dataclass(frozen=True)
class ChemblEntityClientFactoryConfig:
    """Configuration bundle for ChEMBL entity client factories."""

    transport_factory: Callable[[], ApiTransportProtocol]
    pagination_strategy_name: str | None = None
    pagination_strategy: PaginationStrategy | None = None
    pagination_factories: Mapping[str, PaginationFactory] | None = None


class ChemblEntityClientFactoryProtocol(Protocol):
    """Protocol describing factory capable of creating entity clients."""

    config: ChemblEntityClientFactoryConfig

    def create(self, entity: ChemblEntity | str) -> ChemblEntityClientProtocol:
        """Create a client for the specified entity."""

    def __getitem__(self, entity: ChemblEntity | str) -> Callable[[], ChemblEntityClientProtocol]:
        """Provide a callable creator for mapping-style access."""

    def activity(self) -> ChemblEntityClientProtocol:
        ...

    def assay(self) -> ChemblEntityClientProtocol:
        ...

    def target(self) -> ChemblEntityClientProtocol:
        ...

    def testitem(self) -> ChemblEntityClientProtocol:
        ...

    def document(self) -> ChemblEntityClientProtocol:
        ...


def _build_entity_client(
    name: str, entity: ChemblEntity
) -> Type[BaseChemblEntityProtocol]:
    class EntityClient(ChemblEntityClient):
        """Dynamically generated entity client."""

        def __init__(self, transport: ApiTransportProtocol, **kwargs):
            super().__init__(transport, entity, **kwargs)

    EntityClient.__name__ = name
    EntityClient.__qualname__ = name
    return EntityClient


class ChemblEntityClientFactory(ChemblEntityClientFactoryProtocol):
    """Factory for creating configured ChEMBL entity clients.

    Handles dependency injection for transport, pagination, and specific
    entity configuration.
    """

    def __init__(
        self,
        config: ChemblEntityClientFactoryConfig | Callable[[], ApiTransportProtocol],
        *,
        pagination_strategy_name: str | None = None,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_factories: Mapping[str, PaginationFactory] | None = None,
    ):
        if isinstance(config, ChemblEntityClientFactoryConfig):
            self.config = config
        else:
            # Temporary adapter for legacy initialization style
            self.config = ChemblEntityClientFactoryConfig(
                config,
                pagination_strategy_name=pagination_strategy_name,
                pagination_strategy=pagination_strategy,
                pagination_factories=pagination_factories,
            )

    def __getitem__(self, entity: ChemblEntity | str) -> Callable[[], ChemblEntityClientProtocol]:
        return lambda: self.create(entity)

    def create(self, entity: ChemblEntity | str) -> ChemblEntityClientProtocol:
        """Create a client for the specified entity."""
        entity_name = (
            ChemblEntity(entity).value
            if not isinstance(entity, ChemblEntity)
            else entity.value
        )
        return ChemblEntityClient(
            self.config.transport_factory(),
            entity_name,
            pagination_strategy=self.config.pagination_strategy,
            pagination_strategy_name=self.config.pagination_strategy_name,
            pagination_factories=self.config.pagination_factories,
        )

    def _create_specific(
        self, client_cls: Type[BaseChemblEntityProtocol]
    ) -> ChemblEntityClientProtocol:
        # client_cls is a dynamic subclass that binds the 'entity' argument
        # in its __init__, so we don't need to pass it here.
        return client_cls(  # type: ignore[call-arg]
            self.config.transport_factory(),
            pagination_strategy=self.config.pagination_strategy,
            pagination_strategy_name=self.config.pagination_strategy_name,
            pagination_factories=self.config.pagination_factories,
        )

    def activity(self) -> ChemblEntityClientProtocol:
        """Create an activity client."""
        return self._create_specific(ChemblActivityClient)

    def assay(self) -> ChemblEntityClientProtocol:
        """Create an assay client."""
        return self._create_specific(ChemblAssayClient)

    def target(self) -> ChemblEntityClientProtocol:
        """Create a target client."""
        return self._create_specific(ChemblTargetClient)

    def testitem(self) -> ChemblEntityClientProtocol:
        """Create a testitem client."""
        return self._create_specific(ChemblTestItemClient)

    def document(self) -> ChemblEntityClientProtocol:
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
    "ChemblEntityClientProtocol",
    "ChemblEntityClientFactoryConfig",
    "ChemblEntityClientFactoryProtocol",
    "ChemblEntityClient",
    "ChemblEntityClientFactory",
    "CHEMBL_ALLOWED_ENTITIES",
    "ChemblActivityClient",
    "ChemblAssayClient",
    "ChemblTargetClient",
    "ChemblTestItemClient",
    "ChemblDocumentClient",
]
