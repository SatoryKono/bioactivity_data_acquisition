"""Фабрика ChEMBL, делегирующая сборку дескрипторов наружу."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol, TypeVar

from bioetl.clients.base import ClientFactory
from bioetl.clients.chembl.entities import ChemblEntityClientFactory
from bioetl.clients.chembl.factories import default_chembl_factory


T = TypeVar("T")


class ChemblDescriptorFactoryBuilder(Protocol[T]):
    """Контракт на фабрику дескрипторов, инжектируемую извне."""

    def __call__(
        self,
        factory: "ChemblClientFactory[T]",
        entity: str,
        *,
        mode: str | None = None,
    ) -> T:  # pragma: no cover - протокол
        ...


@dataclass
class ChemblClientFactory(ClientFactory[T]):
    """Создаёт ChEMBL-клиентов и делегирует сборку дескрипторов колбэку."""

    config: Mapping[str, Any] | Any
    builder: ChemblDescriptorFactoryBuilder[T]

    def build_entity_client_factory(
        self,
        *,
        pagination_strategy: Any = None,
        pagination_strategy_name: str | None = None,
        pagination_factories: Mapping[str, Any] | None = None,
        transport_factory: Callable[[], Any] | None = None,
    ) -> ChemblEntityClientFactory:
        """Собрать фабрику клиентов ChEMBL с учётом overrides из конфигурации.

        Returns configured ChemblEntityClientFactory instance.
        """

        return default_chembl_factory(
            self.config,
            pagination_strategy=pagination_strategy,
            pagination_strategy_name=pagination_strategy_name,
            pagination_factories=pagination_factories,
            transport_factory=transport_factory,
        )

    def create(self, entity: str, mode: str | None = None) -> T:
        return self.builder(self, entity, mode=mode)


__all__ = ["ChemblClientFactory", "ChemblDescriptorFactoryBuilder"]
