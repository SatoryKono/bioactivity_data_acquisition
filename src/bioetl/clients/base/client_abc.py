"""Базовые абстракции клиентского слоя."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterator, Mapping, Sequence

from bioetl.clients.base.paging import Page, PaginationParams
from bioetl.clients.base.types import Record


@dataclass(slots=True)
class RequestContext:
    """Context information passed to backend implementations."""

    source: str | None = None
    route: str | None = None
    trace_id: str | None = None
    timeout_s: float | None = None
    extra: Mapping[str, object] | None = None


@dataclass(slots=True)
class ClientRequest:
    """Normalized request object passed to data clients."""

    route: str
    ids: Sequence[str] | None = None
    filters: Mapping[str, object] | None = None
    pagination: PaginationParams | None = None
    raw: Mapping[str, object] | None = None
    context: RequestContext | None = None


class BaseClient(ABC):
    """Abstract base class for all high-level data clients."""

    name: str
    source: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        """Base client accepts backend-specific init arguments."""

        super().__init__()

    @abstractmethod
    def fetch_one(self, request: ClientRequest) -> Record | None:
        """Return a single record or None for the given request."""

    @abstractmethod
    def iter_records(self, request: ClientRequest) -> Iterator[Record]:
        """Yield records for the given request."""

    @abstractmethod
    def iter_pages(self, request: ClientRequest) -> Iterator[Page]:
        """Yield pages of records for the given request."""

    @abstractmethod
    def metadata(self) -> Mapping[str, object]:
        """Return client metadata such as configuration details."""

    @abstractmethod
    def close(self) -> None:
        """Release any resources held by the client implementation."""

    def __enter__(self) -> "BaseClient":
        """Enter a context manager for this client instance."""

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> None:
        """Exit the context manager and close the client."""

        self.close()


__all__ = [
    "BaseClient",
    "ClientRequest",
    "Page",
    "PaginationParams",
    "RequestContext",
]
