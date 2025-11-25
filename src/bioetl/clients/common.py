from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from typing import Any, Protocol

from bioetl.base_classes import BaseApiClient

DEFAULT_PAGE_KEY = "results"
DEFAULT_NEXT_KEY = "next"
DEFAULT_PAGE_PARAM = "page"


class PaginationStrategy(Protocol):
    def paginate(self, api_client: BaseApiClient, endpoint: str, **kwargs: Any) -> Iterator[Any]:
        """Iterate over paginated API responses for ``endpoint``."""


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
    ) -> Iterator[Any]:
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
                items = payload.get(page_key)
                if isinstance(items, list) and items:
                    yield from items
                elif payload:
                    yield payload

                next_candidate = payload.get(next_key)
                next_path = next_candidate if isinstance(next_candidate, str) else None
                continue

            if isinstance(payload, Iterable) and not isinstance(payload, (str, bytes, bytearray)):
                yield from payload
            elif payload:
                yield payload


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
    ) -> Iterator[Any]:
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
            if isinstance(payload, Mapping):
                items = payload.get(page_key)
                if isinstance(items, list) and items:
                    yield from items
                elif payload:
                    yield payload
                continue

            if isinstance(payload, Iterable) and not isinstance(payload, (str, bytes, bytearray)):
                yield from payload
            elif payload:
                yield payload


__all__ = [
    "DEFAULT_NEXT_KEY",
    "DEFAULT_PAGE_KEY",
    "DEFAULT_PAGE_PARAM",
    "PaginationStrategy",
    "NextLinkPagination",
    "PageParamPagination",
]
