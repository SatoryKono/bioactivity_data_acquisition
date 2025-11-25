from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any, Protocol, TypeVar

import structlog

from bioetl.base_classes import BaseApiClient, EntityClientProtocol
from bioetl.clients import client_exceptions


_T = TypeVar("_T")
Normalizer = Callable[[Any], Iterator[dict[str, Any]]]


class ApiClientMixin:
    def _normalize_payload(self, payload: Any) -> Iterator[dict[str, Any]]:
        if isinstance(payload, Mapping):
            results = payload.get("results")
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


class ClosableMixin:
    api_client: BaseApiClient

    def close(self) -> None:
        close = getattr(self.api_client, "close", None)
        if callable(close):
            close()

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

DEFAULT_PAGE_KEY = "results"
DEFAULT_NEXT_KEY = "next"
DEFAULT_PAGE_PARAM = "page"


class PaginatedFetcher(Protocol):
    def paginate(
        self,
        api_client: BaseApiClient,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        logger: Any | None = None,
        page_key: str | None = None,
        next_key: str | None = None,
        page_param: str | None = None,
        normalize: Normalizer | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Iterate over paginated API responses for ``endpoint``."""


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


# Backwards-compatibility alias until pagination implementations migrate fully.
PaginationStrategy = PaginatedFetcher


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

    def paginate(
        self,
        api_client: BaseApiClient,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        logger: Any | None = None,
        page_key: str | None = None,
        next_key: str | None = None,
        page_param: str | None = None,
        normalize: Normalizer | None = None,
    ) -> Iterator[dict[str, Any]]:
        del page_param

        page_key = page_key or self.page_key
        next_key = next_key or self.next_key
        next_path = endpoint
        query_params: Mapping[str, Any] | None = dict(params) if params else None

        while next_path:
            payload = api_client.get_json(next_path, params=query_params)
            if logger:
                logger.info("api_call", path=next_path)

            query_params = None
            if isinstance(payload, Mapping):
                yield from _iter_payload_items(payload, page_key=page_key, normalize=normalize)

                next_candidate = payload.get(next_key)
                next_path = next_candidate if isinstance(next_candidate, str) else None
                continue

            yield from _iter_payload_items(payload, page_key=page_key, normalize=normalize)


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

    def paginate(
        self,
        api_client: BaseApiClient,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        logger: Any | None = None,
        page_key: str | None = None,
        next_key: str | None = None,
        page_param: str | None = None,
        normalize: Normalizer | None = None,
    ) -> Iterator[dict[str, Any]]:
        del logger

        page_key = page_key or self.page_key
        next_key = next_key or self.next_key
        page_param = page_param if page_param is not None else self.page_param

        for payload in api_client.paginate_json(
            endpoint,
            params=params,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        ):
            yield from _iter_payload_items(payload, page_key=page_key, normalize=normalize)


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

            for payload in self.pagination_strategy.paginate(
                self.api_client,
                f"/{self.entity}",
                params=query_params,
                logger=self._logger,
                page_key=page_key,
                next_key=next_key,
                page_param=page_param,
            ):
                yield from self._normalize_payload(payload)

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
