from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, MutableMapping, TypeVar

from bioetl.clients.base.contracts import ExternalDataClient
from bioetl.clients.base.http_backend import HttpBackend
from bioetl.clients.base.rest_client import ConfiguredRestClient
from bioetl.clients.config.loader import load_source_config
from bioetl.clients.config.models import SourceConfig

ClientT = TypeVar("ClientT", bound=ExternalDataClient)
BackendFactory = Callable[[SourceConfig], HttpBackend]
ClientBuilder = Callable[[SourceConfig, HttpBackend], ClientT]


@dataclass(slots=True)
class ClientFactoryContext:
    """Контекст создания REST-клиентов."""

    http_backend_factory: BackendFactory | None = None
    registry: MutableMapping[str, ClientBuilder] = field(default_factory=dict)


def default_client_builder(config: SourceConfig, backend: HttpBackend) -> ExternalDataClient:
    return ConfiguredRestClient(config=config, backend=backend)


def create_client(
    source: str,
    *,
    config: SourceConfig | None = None,
    http_backend: HttpBackend | None = None,
    context: ClientFactoryContext | None = None,
) -> ExternalDataClient:
    """Создать REST-клиента из YAML-конфигурации и backend."""

    source_config = config or load_source_config(source)
    ctx = context or ClientFactoryContext()
    backend = http_backend
    if backend is None:
        if ctx.http_backend_factory is None:
            msg = "HttpBackend не передан и фабрика не сконфигурирована"
            raise ValueError(msg)
        backend = ctx.http_backend_factory(source_config)

    builder = ctx.registry.get(source_config.source, default_client_builder)
    return builder(source_config, backend)


__all__ = ["BackendFactory", "ClientBuilder", "ClientFactoryContext", "create_client"]
