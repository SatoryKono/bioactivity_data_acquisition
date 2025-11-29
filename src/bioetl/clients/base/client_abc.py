from __future__ import annotations

"""Базовые абстракции клиентского слоя."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterator, Mapping, Sequence

from bioetl.clients.base.paging import Page, PaginationParams
from bioetl.clients.base.types import Record


@dataclass(slots=True)
class RequestContext:
    source: str | None = None
    route: str | None = None
    trace_id: str | None = None
    timeout_s: float | None = None
    extra: Mapping[str, object] | None = None


@dataclass(slots=True)
class ClientRequest:
    route: str
    ids: Sequence[str] | None = None
    filters: Mapping[str, object] | None = None
    pagination: PaginationParams | None = None
    raw: Mapping[str, object] | None = None
    context: RequestContext | None = None


class BaseClient(ABC):
    name: str
    source: str

    @abstractmethod
    def fetch_one(self, request: ClientRequest) -> Record | None:
        ...

    @abstractmethod
    def iter_records(self, request: ClientRequest) -> Iterator[Record]:
        ...

    @abstractmethod
    def iter_pages(self, request: ClientRequest) -> Iterator[Page]:
        ...

    @abstractmethod
    def metadata(self) -> Mapping[str, object]:
        ...

    @abstractmethod
    def close(self) -> None:
        ...

    def __enter__(self) -> "BaseClient":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> None:
        self.close()


__all__ = ["BaseClient", "ClientRequest", "Page", "PaginationParams", "RequestContext"]
