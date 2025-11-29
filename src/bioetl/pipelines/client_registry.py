"""Реестр фабрик клиентов для пайплайнов."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from bioetl.clients.factory import ClientFactory


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
    _ = config
    factories: dict[str, ClientFactory[Any]] = {}
    if chembl_factory is None:
        msg = "chembl_factory must be provided after legacy factories removal"
        raise ValueError(msg)

    factories["chembl"] = chembl_factory
    if enricher_factory is not None:
        factories["enricher"] = enricher_factory
    return ClientFactoryRegistry(factories)


__all__ = ["ClientFactoryRegistry", "build_client_registry"]
