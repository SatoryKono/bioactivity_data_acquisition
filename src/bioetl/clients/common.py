from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from functools import lru_cache
from typing import Any, Sequence as TypingSequence, TypeVar
import warnings

import structlog

warnings.warn(
    "bioetl.clients.common устаревает как источник ApiClientMixin/ClosableMixin; "
    "используйте bioetl.core.http вместо прямого импорта из clients.common.",
    DeprecationWarning,
    stacklevel=2,
)

from bioetl.core.http import ApiClientMixin, ClosableMixin
from bioetl.core.http.api_entity_client import EntityClientProtocol
from bioetl.core.http.interfaces import ApiTransportProtocol, BaseApiClient
from bioetl.core.http.pagination import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
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

_T = TypeVar("_T")


def iterate_by_ids(
    *,
    ids: Sequence[str],
    entity: str,
    api_client: BaseApiClient,
    normalize: Normalizer,
    wrap_callable: WrapCallable,
    wrap_iterator: WrapIterator,
    logger: structlog.stdlib.BoundLogger | structlog.types.BindableLogger,
    path_template: str = "/{entity}/{id}",
) -> Iterator[dict[str, Any]]:
    def iterator() -> Iterator[dict[str, Any]]:
        for raw_id in ids:
            entity_id = str(raw_id)
            path = path_template.format(entity=entity, id=entity_id)
            payload = wrap_callable(lambda: api_client.get_json(path), log_context={"path": path})
            logger.info("api_call", entity=entity, entity_id=entity_id)
            yield from normalize(payload)

    return wrap_iterator(iterator, log_context=None)


def cache_entity_client(
    client: EntityClientProtocol, *, maxsize: int = 256
) -> EntityClientProtocol:
    """Оборачивает клиент сущности кэширующим декоратором."""

    def _freeze(params: Mapping[str, Any] | None) -> tuple[tuple[str, Any], ...]:
        return tuple(sorted((params or {}).items()))

    @lru_cache(maxsize=maxsize)
    def _cached_get(entity_id: str, frozen_params: tuple[tuple[str, Any], ...]) -> Mapping[str, Any]:
        return client.get(entity_id, params=dict(frozen_params))

    class _CachedEntityClient(ApiClientMixin, ClosableMixin, EntityClientProtocol):
        def __init__(self, wrapped: EntityClientProtocol):
            self._wrapped = wrapped
            self.transport = getattr(wrapped, "transport", None) or getattr(wrapped, "api_client", None)
            self.entity = getattr(wrapped, "entity", "entity")
            self._logger = structlog.get_logger(__name__).bind(entity=self.entity, cache_enabled=True)

        def get(self, entity_id: str, *, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
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
            warnings.warn(
                "fetch_all is deprecated; use list instead to enumerate entities.",
                DeprecationWarning,
                stacklevel=2,
            )
            yield from self.list(
                page_size=page_size,
                params=params,
                page_key=page_key,
                next_key=next_key,
                page_param=page_param,
            )

        def fetch_by_ids(self, ids: TypingSequence[str]) -> Iterator[Mapping[str, Any]]:
            for entity_id in ids:
                yield self.get(str(entity_id))

        def search(self, params: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
            yield from self._wrapped.search(params)

        def close(self) -> None:
            close_fn = getattr(self._wrapped, "close", None)
            if callable(close_fn):
                close_fn()

    return _CachedEntityClient(client)


__all__ = [
    "ApiClientMixin",
    "ApiTransportProtocol",
    "BaseApiClient",
    "ClosableMixin",
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
