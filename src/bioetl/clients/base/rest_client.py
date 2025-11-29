from __future__ import annotations

from collections.abc import Iterator
from typing import Mapping

from bioetl.clients.base.contracts import ClientRequest, ExternalDataClient, Page, Record
from bioetl.clients.base.http_backend import HttpBackend
from bioetl.clients.config.models import ResourceConfig, SourceConfig


class ConfiguredRestClient(ExternalDataClient):
    """Тонкая реализация клиента, основанная на SourceConfig и HttpBackend."""

    def __init__(self, *, config: SourceConfig, backend: HttpBackend) -> None:
        self._config = config
        self._backend = backend

    def fetch_one(self, request: ClientRequest) -> Record | None:
        resource = self._resolve_resource(request.route)
        return self._backend.fetch_one(
            source=self._config, resource=resource, request=request, context=request.context
        )

    def fetch_many(self, request: ClientRequest) -> Iterator[Record]:
        resource = self._resolve_resource(request.route)
        return self._backend.iter_records(
            source=self._config, resource=resource, request=request, context=request.context
        )

    def iter_pages(self, request: ClientRequest) -> Iterator[Page]:
        resource = self._resolve_resource(request.route)
        return self._backend.iter_pages(
            source=self._config, resource=resource, request=request, context=request.context
        )

    def metadata(self) -> Mapping[str, object]:
        return self._backend.metadata(source=self._config)

    def close(self) -> None:
        self._backend.close()

    def _resolve_resource(self, route: str) -> ResourceConfig:
        try:
            return self._config.resources[route]
        except KeyError as exc:
            msg = f"Route '{route}' не найден в SourceConfig {self._config.source}"
            raise ValueError(msg) from exc


__all__ = ["ConfiguredRestClient"]
