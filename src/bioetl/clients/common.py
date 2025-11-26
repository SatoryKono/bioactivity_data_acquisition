from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from functools import lru_cache
from typing import Any, Protocol, Sequence as TypingSequence, TypeVar
import warnings

import structlog

warnings.warn(
    "bioetl.clients.common устаревает как источник ApiClientMixin/ClosableMixin; "
    "используйте bioetl.core.http вместо прямого импорта из clients.common.",
    DeprecationWarning,
    stacklevel=2,
)

from bioetl.clients import client_exceptions
from bioetl.core.http import ApiClientMixin as _ApiClientMixin
from bioetl.core.http import BaseApiEntityClient
from bioetl.core.http import ClosableMixin as _ClosableMixin
from bioetl.core.http.interfaces import ApiTransportProtocol, BaseApiClient
from bioetl.core.http.pagination import PaginationStrategy


JSONPayload = Mapping[str, Any] | list[Mapping[str, Any]]
JSONPage = Iterator[Mapping[str, Any]]
JSONRecord = Mapping[str, Any]
JSONRecordStream = Iterator[JSONRecord]


from bioetl.infra import PaginationRegistry, get_default_pagination_registry
from bioetl.clients.utils import pagination as pagination_utils


class EntityClientProtocol(Protocol):
    entity: str

    def get(self, entity_id: str, *, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        ...

    def list(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[Mapping[str, Any]]:
        ...

    def fetch_by_ids(self, ids: TypingSequence[str]) -> Iterator[Mapping[str, Any]]:
        ...

    def search(self, params: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
        ...

    def close(self) -> None:
        ...


_T = TypeVar("_T")
Normalizer = Callable[[Any], Iterator[dict[str, Any]]]
DEFAULT_PAGE_KEY = pagination_utils.DEFAULT_PAGE_KEY
DEFAULT_NEXT_KEY = pagination_utils.DEFAULT_NEXT_KEY
DEFAULT_PAGE_PARAM = pagination_utils.DEFAULT_PAGE_PARAM

ApiClientMixin = _ApiClientMixin
ClosableMixin = _ClosableMixin
# NOTE: ApiClientMixin/ClosableMixin остаются реэкспортированными здесь ради совместимости,
#       но основным источником следует считать ``bioetl.core.http``.


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
    return pagination_utils.iter_ids(
        ids=ids,
        entity=entity,
        transport=api_client,
        normalize=lambda payload, page_key=None: normalize(payload),
        wrap_callable=wrap_callable,
        wrap_iterator=wrap_iterator,
        logger=logger,
        path_template=path_template,
    )


def _iter_payload_items(
    payload: Any, *, page_key: str, normalize: Normalizer | None
) -> Iterator[dict[str, Any]]:
    if normalize is not None:
        yield from normalize(payload)
        return

    yield from pagination_utils.normalize_payload(payload, page_key=page_key)


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
        transport: ApiTransportProtocol,
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
        transport: ApiTransportProtocol,
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

            if isinstance(response, Mapping) and next_key in response:
                break

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


class ChemblClientBase(BaseApiEntityClient, EntityClientProtocol, ABC):
    """Базовый клиент ChEMBL с общей логикой пагинации и обхода записей."""

    def __init__(
        self,
        transport: ApiTransportProtocol,
        entity: str,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_registry: PaginationRegistry | None = None,
    ) -> None:
        self.pagination_registry = pagination_registry or get_default_pagination_registry()
        pagination = pagination_strategy or self.default_pagination_strategy(
            strategy_name=pagination_strategy_name
        )
        super().__init__(transport, pagination, entity=entity)

    def default_pagination_strategy(self, *, strategy_name: str | None = None) -> PaginationStrategy:
        """Выбор стратегии пагинации через реестр (по умолчанию ``next_link``)."""

        return self.pagination_registry.create(strategy_name or "next_link")


class UnifiedEntityClientBase(ChemblClientBase, ABC):
    """Совместимый псевдоним базового клиента сущностей ChEMBL."""

    def default_pagination_strategy(self, *, strategy_name: str | None = None) -> PaginationStrategy:  # noqa: D401
        """Выбор стратегии пагинации через реестр (по умолчанию ``next_link``)."""

        return super().default_pagination_strategy(strategy_name=strategy_name)


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
            close = getattr(self._wrapped, "close", None)
            if callable(close):
                close()

    return _CachedEntityClient(client)


__all__ = [
    "DEFAULT_NEXT_KEY",
    "DEFAULT_PAGE_KEY",
    "DEFAULT_PAGE_PARAM",
    "ApiClientMixin",
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
    "ChemblClientBase",
    "UnifiedEntityClientBase",
    "ApiTransportProtocol",
    "cache_entity_client",
    "iterate_by_ids",
]

