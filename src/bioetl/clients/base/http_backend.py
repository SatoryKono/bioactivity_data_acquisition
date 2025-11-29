from __future__ import annotations

from collections.abc import Iterator
from typing import Protocol

from bioetl.clients.base.contracts import ClientRequest, Page, Record, RequestContext
from bioetl.clients.config.models import ResourceConfig, SourceConfig


class HttpBackend(Protocol):
    """Контракт HTTP-бэкенда, скрывающего детали транспорта."""

    def fetch_one(
        self,
        *,
        source: SourceConfig,
        resource: ResourceConfig,
        request: ClientRequest,
        context: RequestContext | None = None,
    ) -> Record | None:
        ...

    def iter_records(
        self,
        *,
        source: SourceConfig,
        resource: ResourceConfig,
        request: ClientRequest,
        context: RequestContext | None = None,
    ) -> Iterator[Record]:
        ...

    def iter_pages(
        self,
        *,
        source: SourceConfig,
        resource: ResourceConfig,
        request: ClientRequest,
        context: RequestContext | None = None,
    ) -> Iterator[Page]:
        ...

    def metadata(self, *, source: SourceConfig) -> dict[str, object]:
        ...

    def close(self) -> None:
        ...


__all__ = ["HttpBackend"]
