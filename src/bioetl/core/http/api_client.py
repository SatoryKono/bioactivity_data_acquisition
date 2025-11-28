"""Unified HTTP client for BioETL pipeline components."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, cast

import warnings

import structlog

from bioetl.core.http.client_mixins import ClosableMixin
from bioetl.core.http.config import APIConfig
from bioetl.core.http.interfaces import BaseApiClient
from bioetl.core.http.pagination_helpers import normalize_payload
from bioetl.core.http.resilience import ResilientRequestExecutorFactory


__all__ = ["APIConfig", "UnifiedAPIClient"]


class UnifiedAPIClient(BaseApiClient, ClosableMixin):
    """Simple adapter bundling executor, builder, and pagination strategy."""

    def __init__(
        self,
        api_config: APIConfig,
        *,
        request_executor: Any,
        request_builder: Any,
        pagination_strategy: Any,
        retry_strategy: Any | None = None,
        timeout_seconds: float | None = None,
    ) -> None:
        self.api_config = api_config
        self.request_executor = request_executor
        self.request_builder = request_builder
        self.pagination_strategy = pagination_strategy
        self._retry_strategy = retry_strategy
        self._timeout_seconds = timeout_seconds
        self._logger = structlog.get_logger(__name__)

    @classmethod
    def from_config(
        cls,
        api_config: APIConfig,
        *,
        pagination_strategy: Any = None,
        **kwargs: Any,
    ) -> UnifiedAPIClient:
        """Create a UnifiedAPIClient from an APIConfig."""
        factory = ResilientRequestExecutorFactory(api_config)
        components = factory.create(
            pagination_strategy=pagination_strategy, **kwargs
        )
        return cls(
            api_config=api_config,
            request_executor=components.executor,
            request_builder=components.request_builder,
            pagination_strategy=components.pagination_strategy,
            retry_strategy=components.retry_strategy,
            timeout_seconds=api_config.timeout_sec,
        )

    def fetch_one(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Mapping[str, Any]:
        """Fetch and validate a single JSON object from ``endpoint``."""
        url = self.request_builder.build_url(endpoint)
        merged_headers = self.request_builder.merge_headers(headers)
        payload = self.request_executor.request(
            "GET", url, params=params, headers=merged_headers
        )
        if isinstance(payload, Mapping):
            return cast(Mapping[str, Any], payload)

        msg = (
            "Expected mapping payload when fetching a single resource; "
            f"got {type(payload).__name__}"
        )
        raise TypeError(msg)

    def get_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Mapping[str, Any] | list[Mapping[str, Any]]:
        """Compatibility alias for :meth:`fetch_one`.

        A ``DeprecationWarning`` is emitted to encourage migration to
        ``fetch_one`` while keeping legacy call sites working.
        """

        warnings.warn(
            "get_json is deprecated; use fetch_one for clarity.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.fetch_one(
            endpoint,
            params=params,
            headers=headers,
        )

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
        # Fetch first page to bootstrap pagination strategy
        url = self.request_builder.build_url(endpoint)
        merged_headers = self.request_builder.merge_headers(headers)
        initial_response = self.request_executor.request(
            "GET", url, params=params, headers=merged_headers
        )

        def _normalize(p: Any) -> Iterator[dict[str, Any]]:
            return normalize_payload(p, page_key=page_key)

        pages = self.pagination_strategy.iter_pages(
            initial_response,
            transport=self.request_executor,
            endpoint=url,
            params=params,
            logger=self._logger,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
            normalize=_normalize,
        )

        for page in pages:
            yield from _normalize(page)

    def fetch_batch(
        self,
        *,
        ids: Sequence[str],
        page_size: int | None = None,
        fetcher: Callable[[Sequence[str]], Any] | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        """Fetch a batch of resources using an optional ``fetcher`` callback."""

        def iterator() -> Iterator[Mapping[str, Any]]:
            if callable(fetcher):
                result = fetcher(ids)
                if isinstance(result, Iterator):
                    yield from result
                    return
                if result is not None:
                    yield from normalize_payload(result)
                    return

            if not ids:
                return

            # Delegate to ``iterate_records`` to preserve legacy behavior
            # for consumers that implemented it.
            yield from self.iterate_records(
                ids=ids, page_size=page_size, fetcher=fetcher
            )

        yield from iterator()

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
        # UnifiedAPIClient usually just delegates to paginate_json via default
        # implementation or specific strategy. For generic client, this might
        # not be fully implemented or rely on fetcher.
        # If this method is required by BaseApiClient, we should implement it.
        # However, typical use of iterate_records is in EntityClient which
        # wraps this. For now, returning empty iterator or raising
        # NotImplemented if not used. But BaseApiClient says it yields Mapping.
        if fetcher:
            yield from fetcher(ids)
            return

        # Fallback: cannot iterate without endpoint or fetcher
        msg = (
            "UnifiedAPIClient.iterate_records requires a fetcher or "
            "ids handling logic"
        )
        raise NotImplementedError(msg)

    def close(self) -> None:
        close_fn = getattr(self.request_executor, "close", None)
        if callable(close_fn):
            close_fn()
        close_fn = getattr(self.request_builder, "close", None)
        if callable(close_fn):
            close_fn()

    @property
    def retry_strategy(self) -> Any:
        return (
            getattr(self.request_executor, "retry_strategy", None)
            or self._retry_strategy
        )

    @property
    def timeout_seconds(self) -> float:
        return cast(
            float,
            getattr(self.request_executor, "timeout_seconds", None)
            or self._timeout_seconds
            or self.api_config.timeout_sec,
        )
