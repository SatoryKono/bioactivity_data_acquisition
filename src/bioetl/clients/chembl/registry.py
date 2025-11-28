"""Реестр фабрик фасадов ChEMBL по коду сущности."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

from bioetl.clients.chembl.entities import (
    CHEMBL_ALLOWED_ENTITIES,
    ChemblEntityClientFactory,
    ChemblEntityClientFactoryProtocol,
)
from bioetl.clients.chembl.facade import ChemblClientFacade
from bioetl.clients.chembl.factories import default_chembl_factory
from bioetl.clients.chembl.pagination import PaginationFactory, PaginationStrategy
from bioetl.core.http.interfaces import ApiTransportProtocol

FacadeFactory = Callable[[str], ChemblClientFacade]


def _collect_entity_fetchers(
    context: Mapping[str, Any] | None,
) -> dict[str, Callable[[Any], Any]]:
    fetchers: dict[str, Callable[[Any], Any]] = {}
    if not isinstance(context, Mapping):
        return fetchers
    for entity_code in CHEMBL_ALLOWED_ENTITIES:
        key = f"{entity_code}_fetcher"
        candidate = context.get(key)
        if callable(candidate):
            fetchers[entity_code] = candidate
    return fetchers


@dataclass(slots=True)
class ChemblClientFactoryRegistry:
    """Регистрация фабрик фасадов ChEMBL для сущностей."""

    client_factory: ChemblEntityClientFactoryProtocol
    entity_fetchers: Mapping[str, Callable[[Any], Any]] = field(default_factory=dict)
    default_page_size: int = 1000
    _registry: dict[str, FacadeFactory] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        for entity_code in CHEMBL_ALLOWED_ENTITIES:
            self.register(entity_code, self._build_default_factory(entity_code))

    def _build_default_factory(self, entity_code: str) -> FacadeFactory:
        def factory(name: str = entity_code) -> ChemblClientFacade:
            return ChemblClientFacade(
                name,
                client_factory=self.client_factory,
                entity_fetcher=self.entity_fetchers.get(name),
                default_page_size=self.default_page_size,
            )

        return factory

    def register(self, entity_code: str, factory: FacadeFactory) -> None:
        self._registry[entity_code] = factory

    def create(self, entity_code: str) -> ChemblClientFacade:
        factory = self._registry.get(entity_code)
        if factory is None:
            msg = f"Unknown ChEMBL entity '{entity_code}'"
            raise KeyError(msg)
        return factory(entity_code)

    @classmethod
    def from_config(
        cls,
        config: Mapping[str, Any] | Any,
        *,
        client_factory: ChemblEntityClientFactory | None = None,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_factories: Mapping[str, PaginationFactory] | None = None,
        transport_factory: Callable[[], ApiTransportProtocol] | None = None,
        default_page_size: int = 1000,
    ) -> "ChemblClientFactoryRegistry":
        chembl_factory = client_factory or default_chembl_factory(
            config,
            pagination_strategy=pagination_strategy,
            pagination_strategy_name=pagination_strategy_name,
            pagination_factories=pagination_factories,
            transport_factory=transport_factory,
        )
        context = config.get("sources", {}).get("chembl", {}) if isinstance(
            config, Mapping
        ) else {}
        fetchers = _collect_entity_fetchers(context)
        return cls(
            chembl_factory, entity_fetchers=fetchers, default_page_size=default_page_size
        )


__all__ = ["ChemblClientFactoryRegistry"]
