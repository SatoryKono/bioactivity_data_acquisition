from __future__ import annotations

"""Unified contracts and dataclasses for client layer."""

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Hashable, MutableMapping, Protocol, Sequence, TypeVar, runtime_checkable

Record = MutableMapping[str, Any]
RecordStream = Iterator[Record]


@dataclass(slots=True)
class Page:
    """A single page of low-level results."""

    items: list[Record]
    next_cursor: int | str | None = None
    has_next: bool = False
    raw: Mapping[str, Any] | None = None


PageStream = Iterator[Page]


@dataclass(slots=True, frozen=True)
class PaginationParams:
    """Unified pagination parameters for all sources."""

    page_key: str | None = None
    next_key: str | None = None
    page_param: str | None = None
    page_size: int | None = None

    # Global limits/offsets used by higher-level adapters
    limit: int | None = None
    offset: int | None = None
    max_pages: int | None = None

    def override(self, **kwargs: Any) -> "PaginationParams":
        data = {
            "page_key": self.page_key,
            "next_key": self.next_key,
            "page_param": self.page_param,
            "page_size": self.page_size,
            "limit": self.limit,
            "offset": self.offset,
            "max_pages": self.max_pages,
        }
        data.update({k: v for k, v in kwargs.items() if v is not None})
        return PaginationParams(**data)


@dataclass(slots=True, frozen=True)
class RequestContext:
    """Auxiliary context for logging, tracing and transport tuning."""

    source: str | None = None
    route: str | None = None
    trace_id: str | None = None
    timeout_s: float | None = None
    max_retries: int | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TransportOptions:
    timeout_sec: float | None = None
    headers: Mapping[str, str] | None = None


@dataclass(slots=True)
class RetryOptions:
    max_retries: int | None = None


@runtime_checkable
class SupportsSearch(Protocol):
    def search(
        self,
        query: Mapping[str, Any],
        *,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
    ) -> RecordStream:  # pragma: no cover - protocol
        ...


@runtime_checkable
class SupportsBatch(Protocol):
    def fetch_batch(
        self,
        ids: list[str],
        *,
        params: Mapping[str, Any] | None = None,
        context: RequestContext | None = None,
    ) -> RecordStream:  # pragma: no cover - protocol
        ...


_T_co = TypeVar("_T_co", covariant=True)


@runtime_checkable
class DataProviderProtocol(Protocol[_T_co]):
    """Base contract for low-level data providers."""

    def fetch_one(
        self,
        ref: str,
        *,
        params: Mapping[str, Any] | None = None,
        context: RequestContext | None = None,
    ) -> RecordStream:  # pragma: no cover - protocol
        ...

    def fetch_many(
        self,
        *,
        query: Mapping[str, Any] | None = None,
        page_size: int | None = None,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
    ) -> RecordStream:  # pragma: no cover - protocol
        ...

    def iter_pages(
        self,
        *,
        query: Mapping[str, Any] | None = None,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
    ) -> PageStream:  # pragma: no cover - protocol
        ...

    def configure(
        self,
        *,
        transport: TransportOptions | None = None,
        pagination: PaginationParams | None = None,
        retries: RetryOptions | None = None,
    ) -> "DataProviderProtocol[_T_co]":  # pragma: no cover - protocol
        ...

    def metadata(self) -> Mapping[str, Any]:  # pragma: no cover - protocol
        ...

    def close(self) -> None:  # pragma: no cover - protocol
        ...


@dataclass(frozen=True)
class ClientRequest:
    ids: Sequence[Hashable] | None = None
    filters: Mapping[str, Any] | None = None
    pagination: PaginationParams | None = None
    raw: Any | None = None


class ClientError(Exception):
    """Base exception for client layer."""


@runtime_checkable
class DataClient(Protocol):
    """High-level client contract for domain resources."""

    # For logging/metrics/routing
    name: str
    source: str

    def fetch_one(
        self,
        request: ClientRequest,
        *,
        context: RequestContext | None = None,
    ) -> Record | None:
        ...

    def iter_records(
        self,
        request: ClientRequest,
        *,
        context: RequestContext | None = None,
    ) -> RecordStream:
        ...

    def iter_pages(
        self,
        request: ClientRequest,
        *,
        context: RequestContext | None = None,
    ) -> PageStream:
        ...

    def close(self) -> None:
        ...

    def __enter__(self) -> "DataClient":
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        ...


__all__ = [
    "ClientError",
    "ClientRequest",
    "DataClient",
    "DataProviderProtocol",
    "Page",
    "PageStream",
    "PaginationParams",
    "Record",
    "RecordStream",
    "RequestContext",
    "RetryOptions",
    "SupportsBatch",
    "SupportsSearch",
    "TransportOptions",
]
