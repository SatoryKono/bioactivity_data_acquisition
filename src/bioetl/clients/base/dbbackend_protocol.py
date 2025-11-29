"""Contract for database backends used by the client layer."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Protocol

from bioetl.clients.base.client_abc import (
    ClientRequest,
    RequestContext,
)
from bioetl.clients.base.paging import Page
from bioetl.clients.base.types import Record


class DbBackendProtocol(Protocol):
    def fetch_one(
        self,
        *,
        request: ClientRequest,
        context: RequestContext | None = None,
    ) -> Record | None:
        ...

    def iter_records(
        self,
        *,
        request: ClientRequest,
        context: RequestContext | None = None,
    ) -> Iterator[Record]:
        ...

    def iter_pages(
        self,
        *,
        request: ClientRequest,
        context: RequestContext | None = None,
    ) -> Iterator[Page]:
        ...

    def metadata(self) -> dict[str, object]:
        ...

    def close(self) -> None:
        ...


__all__ = ["DbBackendProtocol"]
