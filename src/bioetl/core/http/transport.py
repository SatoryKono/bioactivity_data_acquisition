from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from bioetl.clients.base.contracts import PageStream, RecordStream, RequestContext


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
    ) -> RecordStream:
        ...

    def iter_pages(
        self,
        *,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        pagination: Any | None = None,
        raw: Any | None = None,
        context: RequestContext | None = None,
    ) -> PageStream:
        ...

    def close(self) -> None:
        ...


__all__ = ["HttpTransport"]
