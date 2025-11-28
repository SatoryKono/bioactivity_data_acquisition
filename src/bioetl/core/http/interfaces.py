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
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Mapping[str, Any] | list[Mapping[str, Any]]:
        """Fetch a single JSON document from ``endpoint``.

        Notes:
            ``get_json`` is a backwards-compatible alias kept for legacy
            call-sites. Prefer :meth:`fetch_one` for new code.

        Args:
            endpoint: Relative path that will be appended to the base URL.
            params: Optional query parameters to include in the request.
            headers: Optional per-call headers merged on top of client defaults.
            timeout_sec: Optional request timeout override; falls back to
                :pyattr:`default_timeout_sec` when omitted.
            max_retries: Optional retry override; falls back to
                :pyattr:`default_max_retries` when omitted.

        Returns:
            Parsed JSON object or list of objects received from the API.
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
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        """Iterate over a paginated collection of JSON pages.

        Notes:
            ``paginate_json`` is preserved as a legacy alias. Prefer
            :meth:`fetch_batch` for clarity and consistent naming.

        Args:
            endpoint: Relative path to the collection resource.
            params: Optional query parameters applied to every page request.
            headers: Optional per-call headers merged on top of client defaults.
            page_key: Key in the response payload containing the records.
            next_key: Key pointing to the next page URL or token.
            page_param: Name of the query parameter controlling the page index
                for cursor-less pagination styles.
            timeout_sec: Optional request timeout override; falls back to
                :pyattr:`default_timeout_sec` when omitted.
            max_retries: Optional retry override; falls back to
                :pyattr:`default_max_retries` when omitted.

        Yields:
            Parsed JSON objects from each page in sequence.
        """

    def fetch_one(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Mapping[str, Any] | list[Mapping[str, Any]]:
        """Retrieve a single JSON document with optional resilience overrides.

        Args:
            endpoint: Relative path that will be appended to the base URL.
            params: Optional query parameters to include in the request.
            headers: Optional per-call headers merged on top of client defaults.
            timeout_sec: Optional request timeout override; falls back to
                :pyattr:`default_timeout_sec` when omitted.
            max_retries: Optional retry override; falls back to
                :pyattr:`default_max_retries` when omitted.

        Returns:
            Parsed JSON object or list of objects received from the API.
        """

    def fetch_batch(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        """Iterate over a collection resource with pagination and resilience.

        Args:
            endpoint: Relative path to the collection resource.
            params: Optional query parameters applied to every page request.
            headers: Optional per-call headers merged on top of client defaults.
            page_key: Key in the response payload containing the records.
            next_key: Key pointing to the next page URL or token.
            page_param: Name of the query parameter controlling the page index
                for cursor-less pagination styles.
            timeout_sec: Optional request timeout override; falls back to
                :pyattr:`default_timeout_sec` when omitted.
            max_retries: Optional retry override; falls back to
                :pyattr:`default_max_retries` when omitted.

        Yields:
            Parsed JSON objects from each page in sequence.
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

    @property
    def pagination_strategy(self) -> PaginationStrategy | None:
        """Pagination strategy used for collection endpoints.

        Returning ``None`` signals that the client does not expose a dedicated
        pagination strategy and expects callers to handle pagination manually.
        """

    @property
    def default_timeout_sec(self) -> float | None:
        """Default request timeout in seconds or ``None`` if not configured."""

    @property
    def default_max_retries(self) -> int | None:
        """Default number of retries applied to resilient calls."""


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
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Mapping[str, Any] | Sequence[Mapping[str, Any]]:
        """Perform a low-level HTTP request and return parsed JSON."""

    def close(self) -> None:
        """Release any underlying transport resources (sessions, pools, etc.).
        """


class ResilientApiClient(BaseApiClient, Protocol):
    """Protocol capturing resilient client configuration surface.

    This protocol is intended for clients that wish to expose resilience
    settings (pagination, retry, timeout) to downstream consumers while still
    satisfying the :class:`BaseApiClient` contract.
    """

    @property
    def pagination_strategy(self) -> PaginationStrategy:
        """Return the pagination strategy configured for the client."""

    @property
    def default_timeout_sec(self) -> float:
        """Return the default timeout configured for requests."""

    @property
    def default_max_retries(self) -> int:
        """Return the default maximum number of retries allowed."""


__all__ = [
    "ApiTransportProtocol",
    "BaseApiClient",
    "CacheStrategy",
    "CircuitBreakerStrategy",
    "PaginationStrategy",
    "ResilientApiClient",
    "RateLimiter",
    "RetryStrategy",
]
