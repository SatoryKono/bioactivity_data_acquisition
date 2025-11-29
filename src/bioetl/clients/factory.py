"""Единая фабрика клиентов."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, MutableMapping

from bioetl.clients.base import BaseClient, ClientRequest
from bioetl.clients.base.http_backend import HttpBackend
from bioetl.clients.config.loader import load_source_config
from bioetl.clients.config.models import ResourceConfig, SourceConfig

BackendFactory = Callable[[SourceConfig], HttpBackend]
ClientBuilder = Callable[[SourceConfig, HttpBackend], BaseClient]


class ConfiguredHttpClient(BaseClient):
    def __init__(self, *, config: SourceConfig, backend: HttpBackend) -> None:
        self._config = config
        self._backend = backend
        self.name = f"{config.source}.default"
        self.source = config.source

    def fetch_one(self, request: ClientRequest):
        resource = self._resolve_resource(request.route)
        return self._backend.fetch_one(
            source=self._config,
            resource=resource,
            request=request,
            context=request.context,
        )

    def iter_records(self, request: ClientRequest):
        resource = self._resolve_resource(request.route)
        return self._backend.iter_records(
            source=self._config,
            resource=resource,
            request=request,
            context=request.context,
        )

    def iter_pages(self, request: ClientRequest):
        resource = self._resolve_resource(request.route)
        return self._backend.iter_pages(
            source=self._config,
            resource=resource,
            request=request,
            context=request.context,
        )

    def metadata(self):
        return self._backend.metadata(source=self._config)

    def close(self) -> None:  # pragma: no cover - trivial
        self._backend.close()

    def _resolve_resource(self, route: str) -> ResourceConfig:
        try:
            return self._config.resources[route]
        except KeyError as exc:  # pragma: no cover - defensive
            msg = (
                f"Route '{route}' не найден в SourceConfig "
                f"{self._config.source}"
            )
            raise ValueError(msg) from exc


@dataclass(slots=True)
class ClientFactory:
    backend_factory: BackendFactory
    registry: MutableMapping[str, ClientBuilder] = field(default_factory=dict)

    def create(
        self,
        source: str,
        *,
        config: SourceConfig | None = None,
        http_backend: HttpBackend | None = None,
    ) -> BaseClient:
        source_config = config or load_source_config(source)
        backend = http_backend or self.backend_factory(source_config)
        builder = self.registry.get(
            source_config.source,
            default_client_builder,
        )
        return builder(source_config, backend)


def default_client_builder(
    config: SourceConfig,
    backend: HttpBackend,
) -> BaseClient:
    return ConfiguredHttpClient(config=config, backend=backend)


__all__ = [
    "BackendFactory",
    "ClientBuilder",
    "ClientFactory",
    "default_client_builder",
]
