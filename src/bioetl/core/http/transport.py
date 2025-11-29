from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any, Protocol

from bioetl.clients.base import RequestContext
from bioetl.clients.base.paging import Page
from bioetl.clients.base.types import Record


class HttpTransport(Protocol):
    def fetch_one(
        self,
        *,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        pagination: Any | None = None,
        raw: Any | None = None,
        context: RequestContext | None = None,
    ) -> Any:
        ...

    def iter_records(
        self,
        *,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        pagination: Any | None = None,
        raw: Any | None = None,
        context: RequestContext | None = None,
    ) -> Iterator[Record]:
        ...

    def iter_pages(
        self,
        *,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        pagination: Any | None = None,
        raw: Any | None = None,
        context: RequestContext | None = None,
    ) -> Iterator[Page]:
        ...

    def close(self) -> None:
        ...


__all__ = ["HttpTransport"]
