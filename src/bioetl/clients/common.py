from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any, TypeVar

import structlog

from bioetl.base_classes import BaseApiClient, EntityClientProtocol
from bioetl.clients import client_exceptions
from bioetl.core.http.pagination import (
    ApiTransportProtocol,
    NextLinkPagination,
    PageParamPagination,
    PaginationStrategy,
)

_T = TypeVar("_T")
Normalizer = Callable[[Any], Iterator[dict[str, Any]]]


DEFAULT_PAGE_KEY = "results"
DEFAULT_NEXT_KEY = "next"
DEFAULT_PAGE_PARAM = "page"


class ApiClientTransport(ApiTransportProtocol):
    def __init__(self, api_client: BaseApiClient) -> None:
        self._api_client = api_client

    def get(self, path: str, *, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        return self._api_client.get_json(path, params=params)


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


class ApiClientMixin:
    def _normalize_payload(
        self, payload: Any, *, page_key: str = DEFAULT_PAGE_KEY
    ) -> Iterator[dict[str, Any]]:
        if isinstance(payload, Mapping):
            results = payload.get(page_key)
            if isinstance(results, Iterable) and not isinstance(results, (str, bytes, bytearray)):
                for item in results:
                    if isinstance(item, Mapping):
                        yield dict(item)
                return

            yield dict(payload)
            return

        if isinstance(payload, Iterable) and not isinstance(payload, (str, bytes, bytearray)):
            for item in payload:
                if isinstance(item, Mapping):
                    yield dict(item)
            return

        if payload is not None:
            yield {"result": payload}

    def _paginate_with_strategy(
        self,
        *,
        path: str,
        params: Mapping[str, Any] | None,
        page_key: str,
        next_key: str,
        page_param: str | None,
        normalize: Normalizer | None,
    ) -> Iterator[dict[str, Any]]:
        transport = ApiClientTransport(self.api_client)
        logger = getattr(self, "_logger", None)

        initial_payload = self._wrap_callable(
            lambda: transport.get(path, params=params), log_context={"path": path}
        )

        if logger:
            logger.info("api_call", path=path)

        pages = self.pagination_strategy.iter_pages(
            initial_payload,
            transport,
            path=path,
            params=dict(params or {}),
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
            logger=logger,
        )

        normalizer = normalize
        if normalizer is None:
            normalizer = lambda payload: self._normalize_payload(payload, page_key=page_key)

        for payload in pages:
            yield from _iter_payload_items(payload, page_key=page_key, normalize=normalizer)

    def _wrap_callable(
        self, func: Callable[[], _T], *, log_context: Mapping[str, Any] | None = None
    ) -> _T:
        try:
            return func()
        except client_exceptions.HTTPError:
            raise
        except Exception as exc:  # noqa: BLE001
            context = dict(log_context or {})
            self._logger.error("api_call_failed", error=str(exc), **context)
            raise client_exceptions.RequestException(str(exc)) from exc

    def _wrap_iterator(
        self, func: Callable[[], Iterator[dict[str, Any]]], *, log_context: Mapping[str, Any] | None = None
    ) -> Iterator[dict[str, Any]]:
        try:
            yield from func()
        except client_exceptions.HTTPError:
            raise
        except Exception as exc:  # noqa: BLE001
            context = dict(log_context or {})
            self._logger.error("api_call_failed", error=str(exc), **context)
            raise client_exceptions.RequestException(str(exc)) from exc


class ClosableMixin:
    api_client: BaseApiClient

    def close(self) -> None:
        close = getattr(self.api_client, "close", None)
        if callable(close):
            close()

    def iter_ids(self, ids: Sequence[str], path_template: str = "/{entity}/{id}") -> Iterator[dict[str, Any]]:
        def iterator() -> Iterator[dict[str, Any]]:
            for raw_id in ids:
                entity_id = str(raw_id)
                path = path_template.format(entity=self.entity, id=entity_id)
                payload = self._wrap_callable(
                    lambda: self.api_client.get_json(path), log_context={"path": path}
                )
                self._logger.info("api_call", entity=self.entity, entity_id=entity_id)
                yield from self._normalize_payload(payload)

        return self._wrap_iterator(iterator)


class UnifiedEntityClientBase(ApiClientMixin, BaseApiClient, EntityClientProtocol, ABC):
    """Общая база для клиентов ChEMBL-подобных сущностей."""

    def __init__(
        self,
        api_client: BaseApiClient,
        entity: str,
        *,
        pagination_strategy: PaginationStrategy | None = None,
    ) -> None:
        self.api_client = api_client
        self.entity = entity.strip("/")
        self._logger = structlog.get_logger(__name__).bind(entity=self.entity)
        self.pagination_strategy = pagination_strategy or self.default_pagination_strategy()

    @abstractmethod
    def default_pagination_strategy(self) -> PaginationStrategy:
        """Выбор стратегии пагинации по умолчанию для конкретного клиента."""

    def fetch_by_ids(self, ids: Sequence[str]) -> Iterator[dict[str, Any]]:
        return self.iter_ids(ids, "/{entity}/{id}")

    def fetch_all(
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

            yield from self._paginate_with_strategy(
                path=f"/{self.entity}",
                params=query_params,
                page_key=page_key,
                next_key=next_key,
                page_param=page_param,
                normalize=None,
            )

        return self._wrap_iterator(iterator)

    def get_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Mapping[str, Any] | list[Mapping[str, Any]]:
        return self.api_client.get_json(endpoint, params=params, headers=headers)

    def paginate_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> Iterator[Mapping[str, Any]]:
        return self.api_client.paginate_json(
            endpoint,
            params=params,
            headers=headers,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        )

    def close(self) -> None:
        close = getattr(self.api_client, "close", None)
        if callable(close):
            close()


__all__ = [
    "DEFAULT_NEXT_KEY",
    "DEFAULT_PAGE_KEY",
    "DEFAULT_PAGE_PARAM",
    "ApiClientMixin",
    "ClosableMixin",
    "PaginationStrategy",
    "NextLinkPagination",
    "PageParamPagination",
    "UnifiedEntityClientBase",
]
