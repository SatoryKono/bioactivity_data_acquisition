from __future__ import annotations

"""Регистрация фабрик клиентов по источникам."""

from dataclasses import dataclass, field
from typing import MutableMapping

from bioetl.clients.base import BaseClient
from bioetl.clients.base.http_backend import HttpBackend
from bioetl.clients.config.models import SourceConfig
from bioetl.clients.factory import BackendFactory, ClientBuilder, ClientFactory, default_client_builder


@dataclass(slots=True)
class ClientRegistry:
    backend_factory: BackendFactory
    builders: MutableMapping[str, ClientBuilder] = field(default_factory=dict)

    def register(self, source: str, builder: ClientBuilder) -> None:
        self.builders[source] = builder

    def create(
        self,
        source: str,
        *,
        config: SourceConfig | None = None,
        http_backend: HttpBackend | None = None,
    ) -> BaseClient:
        factory = ClientFactory(self.backend_factory, registry=self.builders)
        return factory.create(source, config=config, http_backend=http_backend)


def get_registry(
    backend_factory: BackendFactory, *, builders: MutableMapping[str, ClientBuilder] | None = None
) -> ClientRegistry:
    registry = ClientRegistry(backend_factory, builders=builders or {})
    # Регистрируем дефолт для всех источников, если не перекрыт.
    for source in ("chembl", "pubchem", "pubmed", "crossref", "openalex", "semantic_scholar", "uniprot"):
        registry.builders.setdefault(source, default_client_builder)
    return registry


__all__ = ["ClientRegistry", "get_registry"]
