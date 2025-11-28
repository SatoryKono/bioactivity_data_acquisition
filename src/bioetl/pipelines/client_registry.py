"""Реестр фабрик клиентов для пайплайнов."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from bioetl.clients.base import ClientFactory, register_domain_factories
from bioetl.clients.chembl.factory import ChemblClientFactory


@dataclass
class ClientFactoryRegistry:
    """Простой реестр фабрик клиентов по namespace."""

    factories: Mapping[str, ClientFactory[Any]]

    def get(self, name: str) -> ClientFactory[Any]:
        factory = self.factories.get(name)
        if factory is None:
            msg = f"Client factory '{name}' is not registered"
            raise KeyError(msg)
        return factory


def build_client_registry(
    config: Mapping[str, Any] | Any,
    *,
    chembl_factory: ClientFactory[Any] | None = None,
    enricher_factory: ClientFactory[Any] | None = None,
) -> ClientFactoryRegistry:
    factories = register_domain_factories(
        chembl_factory=chembl_factory or ChemblClientFactory(config),
        enricher_factory=enricher_factory,
    )
    return ClientFactoryRegistry(dict(factories))


__all__ = ["ClientFactoryRegistry", "build_client_registry"]
