from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any, Protocol, runtime_checkable


ResponsePayload = Mapping[str, Any]


@runtime_checkable
class ApiTransportProtocol(Protocol):
    """Минимальный контракт транспорта для запросов страниц API."""

    def get(self, path: str, *, params: Mapping[str, Any] | None = None) -> ResponsePayload:
        ...


@runtime_checkable
class PaginationStrategy(Protocol):
    """Контракт для обхода страниц API на уровне транспорта."""

    def iter_pages(
        self,
        initial_response: ResponsePayload,
        transport: ApiTransportProtocol,
        *,
        path: str,
        params: Mapping[str, Any],
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
        logger: Any | None = None,
    ) -> Iterator[ResponsePayload]:
        ...


class NextLinkPagination(PaginationStrategy):
    """Навигация по страницам через ссылку ``next`` в ответе."""

    def __init__(
        self,
        *,
        page_key: str = "results",
        next_key: str = "next",
    ) -> None:
        self.page_key = page_key
        self.next_key = next_key

    def iter_pages(
        self,
        initial_response: ResponsePayload,
        transport: ApiTransportProtocol,
        *,
        path: str,
        params: Mapping[str, Any],
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
        logger: Any | None = None,
    ) -> Iterator[ResponsePayload]:
        del path, params, page_param
        page_key = page_key or self.page_key
        next_key = next_key or self.next_key

        response: ResponsePayload | Any = initial_response
        while True:
            yield response

            if not isinstance(response, Mapping):
                break

            next_path = response.get(next_key)
            if not isinstance(next_path, str):
                break

            response = transport.get(next_path, params=None)
            if logger:
                logger.info("api_call", path=next_path)


class PageParamPagination(PaginationStrategy):
    """Пагинация с параметром страницы и поддержкой ``next`` ссылки."""

    def __init__(
        self,
        *,
        page_param: str | None = "page",
        page_key: str = "results",
        next_key: str = "next",
    ) -> None:
        self.page_param = page_param
        self.page_key = page_key
        self.next_key = next_key

    def iter_pages(
        self,
        initial_response: ResponsePayload,
        transport: ApiTransportProtocol,
        *,
        path: str,
        params: Mapping[str, Any],
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
        logger: Any | None = None,
    ) -> Iterator[ResponsePayload]:
        page_key = page_key or self.page_key
        next_key = next_key or self.next_key
        page_param = page_param if page_param is not None else self.page_param

        response: ResponsePayload | Any = initial_response
        next_page_number: int | None = None

        if page_param is not None:
            current_page = params.get(page_param)
            if isinstance(current_page, int) and current_page > 0:
                next_page_number = current_page + 1
            else:
                next_page_number = 2

        while True:
            yield response

            if not isinstance(response, Mapping):
                break

            next_candidate = response.get(next_key)
            if isinstance(next_candidate, str):
                response = transport.get(next_candidate, params=None)
                if logger:
                    logger.info("api_call", path=next_candidate)
                continue

            if page_param is None:
                break

            items = response.get(page_key)
            if items is None:
                for value in response.values():
                    if isinstance(value, (list, tuple)):
                        items = value
                        break

            if not items:
                break

            next_params = dict(params)
            next_params[page_param] = next_page_number or 1
            try:
                response = transport.get(path, params=next_params)
            except StopIteration:
                break
            if logger:
                logger.info("api_call", path=path, params=next_params)

            if next_page_number is not None:
                next_page_number += 1


DefaultPaginationStrategy = PageParamPagination

__all__ = [
    "ApiTransportProtocol",
    "PaginationStrategy",
    "NextLinkPagination",
    "PageParamPagination",
    "DefaultPaginationStrategy",
]
