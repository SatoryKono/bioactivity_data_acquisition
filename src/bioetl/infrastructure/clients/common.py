"""Common utilities and protocols for infrastructure clients."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from functools import lru_cache
from typing import Any, Sequence as TypingSequence, TypeVar

import structlog
import warnings

# All imports at top level
from bioetl.core.http import ApiClientMixin, ClosableMixin
from bioetl.core.http.api_entity_client import EntityClientProtocol
from bioetl.core.http.interfaces import (
    ApiTransportProtocol,
    BaseApiClient,
)
from bioetl.core.http.pagination import (
    NextLinkPagination,
    PageParamPagination,
    PaginationStrategy,
)
from bioetl.core.http.types import (
    JSONPage,
    JSONPayload,
    JSONRecord,
    JSONRecordStream,
    Normalizer,
    WrapCallable,
    WrapIterator,
)
from bioetl.infrastructure.clients.utils.pagination import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
    iter_ids,
    warn_fetch_all,
)

# Warning must come before usage of deprecated imports
warnings.warn(
    "bioetl.clients.common is deprecated as source of "
    "ApiClientMixin/ClosableMixin; use bioetl.core.http "
    "instead of direct import from clients.common.",
    DeprecationWarning,
    stacklevel=2,
)

_T = TypeVar("_T")


class _BaseClientAdapter(ApiTransportProtocol):
    def __init__(
        self,
        api_client: BaseApiClient,
    ) -> None:
        self._api_client = api_client

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
    ) -> Mapping[str, Any] | Sequence[Mapping[str, Any]]:
        del json
        if method.upper() != "GET":  # pragma: no cover - safety check
            raise ValueError("_BaseClientAdapter supports only GET requests")
        return self._api_client.get_json(
            path,
            params=params,
            headers=headers,
        )


def iterate_by_ids(
    *,
    ids: Sequence[str],
    entity: str,
    api_client: BaseApiClient,
    normalize: Normalizer,
    wrap_callable: WrapCallable,
    wrap_iterator: WrapIterator,
    logger: (
        structlog.stdlib.BoundLogger | structlog.types.BindableLogger
    ),
    path_template: str = "/{entity}/{id}",
) -> Iterator[dict[str, Any]]:
    """Wrapper for iterating over ``ids`` using a
    BaseApiClient-compatible client."""

    return iter_ids(
        ids=ids,
        entity=entity,
        transport=_BaseClientAdapter(api_client),
        normalize=normalize,
        wrap_callable=wrap_callable,
        wrap_iterator=wrap_iterator,
        logger=logger,
        path_template=path_template,
    )


def cache_entity_client(
    client: EntityClientProtocol, *, maxsize: int = 256
) -> EntityClientProtocol:
    """Wrap an entity client with a caching decorator."""

    def _freeze(
        params: Mapping[str, Any] | None
    ) -> tuple[tuple[str, Any], ...]:
        return tuple(sorted((params or {}).items()))

    @lru_cache(maxsize=maxsize)
    def _cached_get(
        entity_id: str,
        frozen_params: tuple[tuple[str, Any], ...],
    ) -> Mapping[str, Any]:
        return client.get(entity_id, params=dict(frozen_params))

    class _CachedEntityClient(
        ApiClientMixin, ClosableMixin, EntityClientProtocol
    ):
        """Cached wrapper for entity clients."""

        def __init__(self, wrapped: EntityClientProtocol):
            self._wrapped = wrapped
            self.transport = (
                getattr(wrapped, "transport", None)
                or getattr(wrapped, "api_client", None)
            )
            self.entity = getattr(wrapped, "entity", "entity")
            self._logger = structlog.get_logger(__name__).bind(
                entity=self.entity,
                cache_enabled=True,
            )

        def get(
            self,
            entity_id: str,
            *,
            params: Mapping[str, Any] | None = None,
        ) -> Mapping[str, Any]:
            """Get entity with caching."""
            return _cached_get(entity_id, _freeze(params))

        def list(
            self,
            *,
            page_size: int = 1000,
            params: Mapping[str, Any] | None = None,
            page_key: str = DEFAULT_PAGE_KEY,
            next_key: str = DEFAULT_NEXT_KEY,
            page_param: str | None = DEFAULT_PAGE_PARAM,
        ) -> Iterator[Mapping[str, Any]]:
            """List entities with pagination."""
            yield from self._wrapped.list(
                page_size=page_size,
                params=params,
                page_key=page_key,
                next_key=next_key,
                page_param=page_param,
            )

        def fetch_all(
            self,
            *,
            page_size: int = 1000,
            params: Mapping[str, Any] | None = None,
            page_key: str = DEFAULT_PAGE_KEY,
            next_key: str = DEFAULT_NEXT_KEY,
            page_param: str | None = DEFAULT_PAGE_PARAM,
        ) -> Iterator[Mapping[str, Any]]:
            """Fetch all entities with warning for large datasets."""
            yield from warn_fetch_all(
                list_entities_fn=lambda: self.list(
                    page_size=page_size,
                    params=params,
                    page_key=page_key,
                    next_key=next_key,
                    page_param=page_param,
                ),
                wrap_iterator=self._wrap_iterator,
            )

        def fetch_by_ids(
            self, ids: TypingSequence[str]
        ) -> Iterator[Mapping[str, Any]]:
            """Fetch entities by IDs using cache."""
            for entity_id in ids:
                yield self.get(str(entity_id))

        def search(
            self, params: Mapping[str, Any]
        ) -> Iterator[Mapping[str, Any]]:
            """Search entities using wrapped client."""
            yield from self._wrapped.search(params)

        def close(self) -> None:
            """Close the wrapped client if possible."""
            close_fn = getattr(self._wrapped, "close", None)
            if callable(close_fn):
                close_fn()

    return _CachedEntityClient(client)


__all__ = [
    "ApiTransportProtocol",
    "BaseApiClient",
    "EntityClientProtocol",
    "JSONPage",
    "JSONPayload",
    "JSONRecord",
    "JSONRecordStream",
    "NextLinkPagination",
    "PageParamPagination",
    "PaginationStrategy",
    "DEFAULT_NEXT_KEY",
    "DEFAULT_PAGE_KEY",
    "DEFAULT_PAGE_PARAM",
    "cache_entity_client",
    "iterate_by_ids",
]
