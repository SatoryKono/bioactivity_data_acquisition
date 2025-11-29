from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, cast

import structlog

from bioetl.core.http.client_mixins import ApiClientMixin, ClosableMixin
from bioetl.core.http.interfaces import BaseApiClient
from bioetl.core.http.pagination_helpers import normalize_payload

try:  # TYPE_CHECKING guard without importing at runtime
    from bioetl.core.http.interfaces import ApiTransportProtocol  # pragma: no cover
except Exception:  # pragma: no cover - defensive import
    ApiTransportProtocol = Any  # type: ignore[assignment]


class BaseHttpClient(ApiClientMixin, ClosableMixin):
    """Общий HTTP-клиент с едиными таймаутами, ретраями и логированием.

    Класс выступает обёрткой над ``BaseApiClient`` или ``ApiTransportProtocol``,
    проксируя вызовы ``fetch_one``/``fetch_batch``/``iterate_records`` и
    обеспечивая единообразное применение таймаутов/ретраев и контекстного
    логирования.
    """

    def __init__(
        self,
        transport: BaseApiClient | ApiTransportProtocol,
        *,
        default_timeout_sec: float | None = None,
        default_max_retries: int | None = None,
        client_name: str = "http_client",
    ) -> None:
        self._transport = transport
        self._logger = structlog.get_logger(__name__).bind(client=client_name)
        self._default_timeout_sec = (
            default_timeout_sec
            if default_timeout_sec is not None
            else getattr(transport, "default_timeout_sec", None)
        )
        self._default_max_retries = (
            default_max_retries
            if default_max_retries is not None
            else getattr(transport, "default_max_retries", None)
        )
        self._pagination_strategy = getattr(transport, "pagination_strategy", None)

    @property
    def pagination_strategy(self) -> Any | None:  # pragma: no cover - passthrough
        return getattr(self, "_pagination_strategy", None)

    @pagination_strategy.setter
    def pagination_strategy(self, strategy: Any | None) -> None:
        self._pagination_strategy = strategy

    def _resolve_timeout(self, timeout_sec: float | None) -> float | None:
        return timeout_sec if timeout_sec is not None else self._default_timeout_sec

    def _resolve_retries(self, max_retries: int | None) -> int | None:
        return (
            max_retries if max_retries is not None else self._default_max_retries
        )

    def _perform_request(
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
        transport = getattr(self, "_transport")
        requester = getattr(transport, "request", None)
        if not callable(requester):
            msg = "Underlying transport does not implement request"
            raise AttributeError(msg)
        return cast(
            Mapping[str, Any] | Sequence[Mapping[str, Any]],
            requester(
                method,
                path,
                headers=headers,
                params=params,
                json=json,
                timeout_sec=timeout_sec,
                max_retries=max_retries,
            ),
        )

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
        """Выполнить HTTP-запрос с единым логированием и настройками."""

        effective_timeout = self._resolve_timeout(timeout_sec)
        effective_retries = self._resolve_retries(max_retries)
        return self._wrap_callable(
            lambda: self._perform_request(
                method,
                path,
                headers=headers,
                params=params,
                json=json,
                timeout_sec=effective_timeout,
                max_retries=effective_retries,
            ),
            log_context={"path": path, "method": method},
        )

    def get_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Mapping[str, Any] | list[Mapping[str, Any]]:
        """Получить один JSON-документ."""

        return cast(
            Mapping[str, Any] | list[Mapping[str, Any]],
            self.request(
                "GET",
                endpoint,
                params=params,
                headers=headers,
                timeout_sec=timeout_sec,
                max_retries=max_retries,
            ),
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
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        """Итерировать страницы JSON через стратегию пагинации."""

        transport = getattr(self, "_transport")
        effective_timeout = self._resolve_timeout(timeout_sec)
        effective_retries = self._resolve_retries(max_retries)

        delegate = getattr(transport, "paginate_json", None)
        if callable(delegate):
            return iter(
                delegate(
                    endpoint,
                    params=params,
                    headers=headers,
                    page_key=page_key,
                    next_key=next_key,
                    page_param=page_param,
                    timeout_sec=effective_timeout,
                    max_retries=effective_retries,
                )
            )

        strategy = getattr(self, "pagination_strategy", None)
        if strategy is None:
            msg = "Pagination is not supported by the underlying transport"
            raise NotImplementedError(msg)

        first_page = self.request(
            "GET",
            endpoint,
            params=params,
            headers=headers,
            timeout_sec=effective_timeout,
            max_retries=effective_retries,
        )

        def _normalize(payload: Any) -> Iterator[dict[str, Any]]:
            return normalize_payload(payload, page_key=page_key)

        pages = strategy.iter_pages(
            first_page,
            cast(ApiTransportProtocol, transport),
            endpoint=endpoint,
            params=params,
            logger=self._logger,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
            normalize=_normalize,
        )
        return iter(pages)

    def fetch_one(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Mapping[str, Any] | list[Mapping[str, Any]]:
        """Прокси для единичного запроса."""

        delegate = getattr(self._transport, "fetch_one", None)
        if callable(delegate):
            return cast(
                Mapping[str, Any] | list[Mapping[str, Any]],
                delegate(
                    endpoint,
                    params=params,
                    headers=headers,
                    timeout_sec=self._resolve_timeout(timeout_sec),
                    max_retries=self._resolve_retries(max_retries),
                ),
            )
        return self.get_json(
            endpoint,
            params=params,
            headers=headers,
            timeout_sec=timeout_sec,
            max_retries=max_retries,
        )

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
        """Прокси для пагинированного запроса."""

        delegate = getattr(self._transport, "fetch_batch", None)
        if callable(delegate):
            return iter(
                delegate(
                    endpoint,
                    params=params,
                    headers=headers,
                    page_key=page_key,
                    next_key=next_key,
                    page_param=page_param,
                    timeout_sec=self._resolve_timeout(timeout_sec),
                    max_retries=self._resolve_retries(max_retries),
                )
            )

        return self.paginate_json(
            endpoint,
            params=params,
            headers=headers,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
            timeout_sec=timeout_sec,
            max_retries=max_retries,
        )

    def iterate_records(
        self,
        *,
        ids: Sequence[str] | None = None,
        page_size: int | None = None,
        fetcher: Callable[[Sequence[str] | None], Any] | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        delegate = getattr(self._transport, "iterate_records", None)
        if callable(delegate):
            return iter(delegate(ids=ids, page_size=page_size, fetcher=fetcher))

        if fetcher:
            return iter(fetcher(ids))

        msg = "iterate_records is not implemented by the underlying transport"
        raise NotImplementedError(msg)

    def close(self) -> None:  # pragma: no cover - delegated cleanup
        transport = getattr(self, "_transport", None)
        close_fn = getattr(transport, "close", None)
        if callable(close_fn):
            close_fn()
            return
        super().close()


__all__ = ["BaseHttpClient"]
