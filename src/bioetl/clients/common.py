from __future__ import annotations

from collections.abc import Iterator, Mapping, MutableMapping
from typing import Any, Protocol, runtime_checkable

from bioetl.base_classes import BaseApiClient


@runtime_checkable
class PaginationStrategy(Protocol):
    def paginate(
        self,
        api_client: BaseApiClient,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        page_size: int = 1000,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
        logger: Any | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        ...


class NextLinkPagination:
    def paginate(
        self,
        api_client: BaseApiClient,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        page_size: int = 1000,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
        logger: Any | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        next_path: str | None = endpoint
        query_params: MutableMapping[str, Any] = {"limit": page_size, **(params or {})}

        while next_path:
            payload = api_client.get_json(next_path, params=query_params)
            if logger:
                logger.info("api_call", path=next_path)
            yield payload

            query_params = {}
            if not isinstance(payload, Mapping):
                break

            next_candidate = payload.get(next_key)
            next_path = next_candidate if isinstance(next_candidate, str) else None


class PageParamPagination:
    def paginate(
        self,
        api_client: BaseApiClient,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        page_size: int = 1000,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
        logger: Any | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        query_params: MutableMapping[str, Any] = {"limit": page_size, **(params or {})}

        for payload in api_client.paginate_json(
            endpoint,
            params=query_params,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        ):
            if logger:
                logger.info("api_call", path=endpoint)
            yield payload


__all__ = ["PaginationStrategy", "NextLinkPagination", "PageParamPagination"]
