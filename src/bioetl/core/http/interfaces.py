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

    def get_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Mapping[str, Any] | list[Mapping[str, Any]]:
        """Fetch a single resource from ``endpoint`` and return decoded JSON.
        """

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
        """Iterate over paginated JSON resources for the given ``endpoint``.
        """

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
