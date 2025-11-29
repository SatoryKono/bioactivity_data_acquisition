from __future__ import annotations

"""Unified contract for external data clients."""

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, TypedDict, runtime_checkable


class Record(TypedDict, total=False):
    """Минимально типизированная запись внешнего источника."""


@dataclass(slots=True)
class RequestContext:
    """Контекст выполнения запроса и опции транспорта."""

    trace_id: str | None = None
    options: Mapping[str, Any] | None = None


@dataclass(slots=True)
class Pagination:
    """Единые параметры пагинации для запроса."""

    page_size: int | None = None
    cursor: str | None = None


@dataclass(slots=True)
class ClientRequest:
    """Описание запроса к маршруту внешнего источника."""

    route: str
    params: Mapping[str, Any] | None = None
    context: RequestContext | None = None
    pagination: Pagination | None = None


@dataclass(slots=True)
class Page:
    """Страница результатов без нормализации."""

    items: list[Record]
    next_cursor: str | None = None
    raw: Any | None = None


@runtime_checkable
class ExternalDataClient(Protocol):
    """Единый контракт для всех клиентов внешних источников."""

    def fetch_one(self, request: ClientRequest) -> Record | None:
        ...

    def fetch_many(self, request: ClientRequest) -> Iterator[Record]:
        ...

    def iter_pages(self, request: ClientRequest) -> Iterator[Page]:
        ...

    def metadata(self) -> Mapping[str, Any]:
        ...

    def close(self) -> None:
        ...


__all__ = [
    "Record",
    "RequestContext",
    "Pagination",
    "ClientRequest",
    "Page",
    "ExternalDataClient",
]
