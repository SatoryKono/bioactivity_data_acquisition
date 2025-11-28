from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, Protocol, runtime_checkable

from bioetl.core.http.cache import CacheStrategy
from bioetl.core.http.circuit_breaker import CircuitBreakerStrategy
from bioetl.core.http.pagination import PaginationStrategy
from bioetl.core.http.rate_limiter import RateLimiter
from bioetl.core.http.retry import RetryStrategy


@runtime_checkable
class BaseApiClient(Protocol):
    """Protocol describing the minimal HTTP client surface area."""

    def fetch_one(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Mapping[str, Any]:
        """Fetch a single JSON object from ``endpoint``.

        Implementations SHOULD return a mapping representing a single record
        or raise an error if the endpoint responds with a non-mapping payload.
        This mirrors typical REST semantics where a ``GET`` on a resource
        identifier yields a single document.
        """

    def fetch_batch(
        self,
        *,
        ids: Sequence[str],
        page_size: int | None = None,
        fetcher: Callable[[Sequence[str]], Any] | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        """Fetch a batch of records by identifiers.

        ``fetcher`` can be provided to override the default batch retrieval
        logic. Implementations are encouraged to reuse pagination helpers to
        avoid duplicating batching logic across clients.
        """

    def get_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Mapping[str, Any] | list[Mapping[str, Any]]:
        """Alias for :meth:`fetch_one` kept for backward compatibility."""

    def paginate_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[Mapping[str, Any]]:
        """Iterate over paginated JSON resources for the given ``endpoint``."""

    def iterate_records(
        self,
        *,
        ids: Sequence[str] | None = None,
        page_size: int | None = None,
        fetcher: Callable[[Sequence[str] | None], Any] | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        """Yield normalized records, optionally using ``ids`` or a custom
        ``fetcher``.
        """

    @property
    def pagination_strategy(self) -> PaginationStrategy:
        """Pagination strategy used by the client."""

    @property
    def retry_strategy(self) -> RetryStrategy:
        """Retry strategy applied to HTTP requests."""

    @property
    def timeout_seconds(self) -> float:
        """HTTP request timeout in seconds."""

    def close(self) -> None:
        """Release any resources (e.g. sessions) associated with the client."""


@runtime_checkable
class ApiTransportProtocol(Protocol):
    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
    ) -> Mapping[str, Any] | Sequence[Mapping[str, Any]]:
        """Perform a low-level HTTP request and return parsed JSON."""

    @property
    def retry_strategy(self) -> RetryStrategy:
        """Retry strategy applied to the transport."""

    @property
    def timeout_seconds(self) -> float:
        """HTTP request timeout in seconds for this transport."""

    def close(self) -> None:
        """Release any underlying transport resources (sessions, pools, etc.).
        """


__all__ = [
    "ApiTransportProtocol",
    "BaseApiClient",
    "CacheStrategy",
    "CircuitBreakerStrategy",
    "PaginationStrategy",
    "RateLimiter",
    "RetryStrategy",
]
