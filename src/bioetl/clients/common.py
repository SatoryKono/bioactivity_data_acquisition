from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from functools import lru_cache
from typing import Any, Sequence as TypingSequence, TypeVar

import structlog

from bioetl.base_classes import BaseApiClient
from bioetl.clients import client_exceptions
from bioetl.core.http.client_mixins import ApiClientMixin, ClosableMixin
from bioetl.core.http.pagination import PaginationStrategy


class EntityClientProtocol(ABC):
    entity: str

    def get(self, entity_id: str, *, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        raise NotImplementedError

    def list(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[Mapping[str, Any]]:
        raise NotImplementedError

    def fetch_by_ids(self, ids: TypingSequence[str]) -> Iterator[Mapping[str, Any]]:
        raise NotImplementedError

    def search(self, params: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


_T = TypeVar("_T")
Normalizer = Callable[[Any], Iterator[dict[str, Any]]]
_T = TypeVar("_T")
DEFAULT_PAGE_KEY = "results"
DEFAULT_NEXT_KEY = "next"
DEFAULT_PAGE_PARAM = "page"


def iterate_by_ids(
    *,
    ids: Sequence[str],
    entity: str,
    api_client: BaseApiClient,
    normalize: Normalizer,
    wrap_callable: Callable[[Callable[[], _T], Mapping[str, Any] | None], _T],
    wrap_iterator: Callable[[Callable[[], Iterator[dict[str, Any]]], Mapping[str, Any] | None], Iterator[dict[str, Any]]],
    logger: structlog.stdlib.BoundLogger | structlog.types.BindableLogger,
    path_template: str = "/{entity}/{id}",
) -> Iterator[dict[str, Any]]:
    def iterator() -> Iterator[dict[str, Any]]:
        for raw_id in ids:
            entity_id = str(raw_id)
            path = path_template.format(entity=entity, id=entity_id)
            payload = wrap_callable(
                lambda: api_client.request("GET", path), log_context={"path": path, "method": "GET"}
            )
            logger.info("api_call", entity=entity, entity_id=entity_id)
            yield from normalize(payload)

    return wrap_iterator(iterator)


def _iter_payload_items(
    payload: Any, *, page_key: str, normalize: Normalizer | None
) -> Iterator[dict[str, Any]]:
    if normalize is not None:
        yield from normalize(payload)
        return

    if isinstance(payload, Mapping):
        items = payload.get(page_key)
        if isinstance(items, list) and items:
            yield from items
        elif payload:
            yield payload
        return

    if isinstance(payload, Iterable) and not isinstance(payload, (str, bytes, bytearray)):
        yield from payload
        return

    if payload:
        yield payload


class NextLinkPagination:
    """Follow ChEMBL-style pagination using ``next`` links in responses."""

    def __init__(
        self,
        *,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
    ) -> None:
        self.page_key = page_key
        self.next_key = next_key

    def iter_pages(
        self,
        initial_response: Mapping[str, Any] | Sequence[Mapping[str, Any]],
        transport: BaseApiClient,
        *,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        logger: Any | None = None,
        page_key: str | None = None,
        next_key: str | None = None,
        page_param: str | None = None,
        normalize: Normalizer | None = None,
    ) -> Iterator[Mapping[str, Any] | Sequence[Mapping[str, Any]]]:
        del page_param, normalize

        page_key = page_key or self.page_key
        next_key = next_key or self.next_key
        next_path = endpoint
        response: Mapping[str, Any] | Sequence[Mapping[str, Any]] = initial_response
        query_params: Mapping[str, Any] | None = dict(params) if params else None

        while next_path:
            yield response

            if not isinstance(response, Mapping):
                break

            next_candidate = response.get(next_key)
            if not isinstance(next_candidate, str) or not next_candidate:
                break

            next_path = next_candidate
            query_params = None
            response = transport.request("GET", next_path, params=query_params)
            if logger:
                logger.info("api_call", path=next_path)


class PageParamPagination:
    """Paginate using page-number parameter via ``paginate_json`` helper."""

    def __init__(
        self,
        *,
        page_param: str | None = DEFAULT_PAGE_PARAM,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
    ) -> None:
        self.page_param = page_param
        self.page_key = page_key
        self.next_key = next_key

    def iter_pages(
        self,
        initial_response: Mapping[str, Any] | Sequence[Mapping[str, Any]],
        transport: BaseApiClient,
        *,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        logger: Any | None = None,
        page_key: str | None = None,
        next_key: str | None = None,
        page_param: str | None = None,
        normalize: Normalizer | None = None,
    ) -> Iterator[Mapping[str, Any] | Sequence[Mapping[str, Any]]]:
        page_key = page_key or self.page_key
        next_key = next_key or self.next_key
        page_param = page_param if page_param is not None else self.page_param

        page_num = 1
        next_path = endpoint
        query_params = dict(params) if params else {}
        response: Mapping[str, Any] | Sequence[Mapping[str, Any]] = initial_response

        while next_path:
            yield response

            next_candidate = response.get(next_key) if isinstance(response, Mapping) else None
            if isinstance(next_candidate, str) and next_candidate:
                next_path = next_candidate
                query_params = {}
                page_num += 1
                response = transport.request("GET", next_path, params=None)
                if logger:
                    logger.info("api_call", path=next_path)
                continue

            page_items = list(_iter_payload_items(response, page_key=page_key, normalize=normalize))
            if not page_items:
                break

            page_num += 1
            effective_params = dict(query_params)
            if page_param is not None:
                effective_params[page_param] = page_num
            response = transport.request("GET", next_path, params=effective_params)
            if logger:
                logger.info("api_call", path=next_path)


class UnifiedEntityClientBase(ApiClientMixin, ClosableMixin, EntityClientProtocol, ABC):
    """Общая база для клиентов ChEMBL-подобных сущностей."""

    def __init__(
        self,
        transport: BaseApiClient,
        entity: str,
        *,
        pagination_strategy: PaginationStrategy | None = None,
    ) -> None:
        self.transport = transport
        self.entity = entity.strip("/")
        self._logger = structlog.get_logger(__name__).bind(entity=self.entity)
        self.pagination_strategy = pagination_strategy or self.default_pagination_strategy()

    @abstractmethod
    def default_pagination_strategy(self) -> PaginationStrategy:
        """Выбор стратегии пагинации по умолчанию для конкретного клиента."""

    def _entity_path(self, suffix: str | None = None) -> str:
        if not suffix:
            return f"/{self.entity}"
        suffix = str(suffix).lstrip("/")
        return f"/{self.entity}/{suffix}"

    def get(self, entity_id: str, *, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        return self._wrap_callable(
            lambda: self.transport.request("GET", self._entity_path(entity_id), params=params),
            log_context={"path": self._entity_path(entity_id)},
        )

    def fetch_by_ids(self, ids: Sequence[str]) -> Iterator[dict[str, Any]]:
        return iterate_by_ids(
            ids=ids,
            entity=self.entity,
            api_client=self.transport,
            normalize=self._normalize_payload,
            wrap_callable=self._wrap_callable,
            wrap_iterator=self._wrap_iterator,
            logger=self._logger,
        )

    def list(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> Iterator[dict[str, Any]]:
        def iterator() -> Iterator[dict[str, Any]]:
            query_params: dict[str, Any] = {"limit": page_size}
            if params:
                query_params.update(params)

            first_payload = self._wrap_callable(
                lambda: self.transport.request("GET", self._entity_path(), params=query_params),
                log_context={"path": self._entity_path()},
            )
            self._logger.info("api_call", path=self._entity_path())

            for page in self.pagination_strategy.iter_pages(
                first_payload,
                self.transport,
                endpoint=self._entity_path(),
                params=query_params,
                logger=self._logger,
                page_key=page_key,
                next_key=next_key,
                page_param=page_param,
                normalize=self._normalize_payload,
            ):
                yield from _iter_payload_items(page, page_key=page_key, normalize=None)

        return self._wrap_iterator(iterator)

    def fetch_all(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> Iterator[dict[str, Any]]:
        return self.list(
            page_size=page_size,
            params=params,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        )

    def search(self, params: Mapping[str, Any]) -> Iterator[dict[str, Any]]:
        return self.list(params=params)


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

        def fetch_by_ids(self, ids: TypingSequence[str]) -> Iterator[Mapping[str, Any]]:
            for entity_id in ids:
                yield self.get(str(entity_id))

        def search(self, params: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
            yield from self._wrapped.search(params)

        def close(self) -> None:
            close = getattr(self._wrapped, "close", None)
            if callable(close):
                close()

    return _CachedEntityClient(client)


__all__ = [
    "DEFAULT_NEXT_KEY",
    "DEFAULT_PAGE_KEY",
    "DEFAULT_PAGE_PARAM",
    "ApiClientMixin",
    "ClosableMixin",
    "iterate_by_ids",
    "PaginationStrategy",
    "NextLinkPagination",
    "PageParamPagination",
    "UnifiedEntityClientBase",
    "EntityClientProtocol",
    "cache_entity_client",
]
