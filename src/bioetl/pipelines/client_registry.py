"""Реестр фабрик клиентов для пайплайнов."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from bioetl.clients.chembl.factory import ChemblClientFactory


@dataclass
class ClientFactoryRegistry:
    """Простой реестр фабрик клиентов по namespace."""

    factories: Mapping[str, Any]

    def get(self, name: str) -> Any:
        factory = self.factories.get(name)
        if factory is None:
            msg = f"Client factory '{name}' is not registered"
            raise KeyError(msg)
        return factory


def build_client_registry(
    config: Mapping[str, Any] | Any,
    *,
    chembl_factory: ChemblClientFactory | None = None,
) -> ClientFactoryRegistry:
    registry_factory = chembl_factory or ChemblClientFactory(config)
    return ClientFactoryRegistry({"chembl": registry_factory})


__all__ = ["ClientFactoryRegistry", "build_client_registry"]
