"""Базовая реализация унифицированного клиента данных.

Класс ``BaseDataProvider`` инкапсулирует транспортный слой и общую
логика пагинации, предоставляя методы ``fetch_one``/``fetch_many`` и
``iter_pages`` для потокового доступа к сырым записям.
"""
from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

import structlog

from bioetl.clients.base import (
    DataProviderProtocol,
    Page,
    PageStream,
    PaginationParams,
    RecordStream,
    RequestContext,
    RetryOptions,
    TransportOptions,
)
from bioetl.clients.base import exceptions
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
    DefaultPaginationStrategy,
    PaginationStrategy,
)
from bioetl.core.http.pagination_helpers import iter_pages, normalize_payload


class BaseDataProvider(DataProviderProtocol[dict[str, Any]]):
    """Тонкая обёртка над ``ApiTransportProtocol`` с единым API."""

    def __init__(
        self,
        transport: ApiTransportProtocol,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        default_pagination: PaginationParams | None = None,
        source: str | None = None,
        route: str | None = None,
    ) -> None:
        self.transport = transport
        self._pagination_strategy = (
            pagination_strategy
            or getattr(transport, "pagination_strategy", None)
            or DefaultPaginationStrategy()
        )
        self._pagination_params = default_pagination or PaginationParams(
            page_key=DEFAULT_PAGE_KEY,
            next_key=DEFAULT_NEXT_KEY,
            page_param=DEFAULT_PAGE_PARAM,
        )
        self._logger = structlog.get_logger(__name__).bind(
            source=source or "unknown", route=route
        )
        self._transport_options = TransportOptions()
        self._retry_options = RetryOptions()

    def configure(
        self,
        *,
        transport: TransportOptions | None = None,
        pagination: PaginationParams | None = None,
        retries: RetryOptions | None = None,
    ) -> "BaseDataProvider":
        if transport:
            self._transport_options = transport
        if pagination:
            self._pagination_params = pagination
        if retries:
            self._retry_options = retries
        return self

    def _resolve_pagination(
        self, pagination: PaginationParams | None
    ) -> PaginationParams:
        if pagination is None:
            return self._pagination_params
        return self._pagination_params.override(
            page_key=pagination.page_key,
            next_key=pagination.next_key,
            page_param=pagination.page_param,
            page_size=pagination.page_size,
        )

    def _normalize_page(self, payload: Any, page_key: str | None) -> list[dict[str, Any]]:
        return list(normalize_payload(payload, page_key=page_key))

    def fetch_one(
        self,
        ref: str,
        *,
        params: Mapping[str, Any] | None = None,
        context: RequestContext | None = None,
    ) -> RecordStream:
        log_ctx = {"ref": ref, **(context.extra if context else {})}

        def iterator() -> Iterator[dict[str, Any]]:
            payload = self.transport.request(
                "GET",
                ref,
                params=params,
                timeout_sec=self._transport_options.timeout_sec,
                max_retries=self._retry_options.max_retries,
            )
            self._logger.info("api_call", ref=ref)
            yield from normalize_payload(payload, page_key=None)

        try:
            yield from iterator()
        except exceptions.RequestException:
            raise
        except Exception as exc:  # noqa: BLE001
            self._logger.error("provider_fetch_one_failed", error=str(exc), **log_ctx)
            raise exceptions.ProviderError(str(exc)) from exc

    def iter_pages(
        self,
        *,
        query: Mapping[str, Any] | None = None,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
    ) -> PageStream:
        effective_pagination = self._resolve_pagination(pagination)
        params = dict(query or {})
        if effective_pagination.page_size and "limit" not in params:
            params["limit"] = effective_pagination.page_size

        log_ctx = {"params": params, **(context.extra if context else {})}

        try:
            first_payload = self.transport.request(
                "GET",
                "/",
                params=params,
                timeout_sec=self._transport_options.timeout_sec,
                max_retries=self._retry_options.max_retries,
            )
        except Exception as exc:  # noqa: BLE001
            self._logger.error("provider_first_page_failed", error=str(exc), **log_ctx)
            raise exceptions.ProviderError(str(exc)) from exc

        strategy = self._pagination_strategy
        page_key = effective_pagination.page_key or DEFAULT_PAGE_KEY
        next_key = effective_pagination.next_key or DEFAULT_NEXT_KEY
        page_param = effective_pagination.page_param

        for raw_page in iter_pages(
            strategy,
            first_payload,
            self.transport,
            endpoint="/",
            params=params,
            logger=self._logger,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
            normalize=None,
        ):
            items = self._normalize_page(raw_page, page_key=page_key)
            next_cursor = raw_page.get(next_key) if isinstance(raw_page, Mapping) else None
            yield Page(items=items, next_cursor=next_cursor, raw=raw_page if isinstance(raw_page, Mapping) else None)

    def fetch_many(
        self,
        *,
        query: Mapping[str, Any] | None = None,
        page_size: int | None = None,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
    ) -> RecordStream:
        effective_pagination = self._resolve_pagination(pagination)
        if page_size is not None:
            effective_pagination = effective_pagination.override(page_size=page_size)

        for page in self.iter_pages(
            query=query, pagination=effective_pagination, context=context
        ):
            yield from page.items

    def metadata(self) -> Mapping[str, Any]:
        meta = getattr(self.transport, "metadata", None)
        return meta if isinstance(meta, Mapping) else {}

    def close(self) -> None:
        close_fn = getattr(self.transport, "close", None)
        if callable(close_fn):
            close_fn()


__all__ = ["BaseDataProvider"]
