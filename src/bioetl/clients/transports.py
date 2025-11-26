from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Any, Mapping, Sequence

import requests

from bioetl.base_classes import BaseApiClient
from bioetl.clients.common import PageParamPagination


class RequestsTransport(BaseApiClient):
    """Пример синхронного транспорта на базе ``requests``."""

    def __init__(self, base_url: str, session: requests.Session | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self._session = session or requests.Session()

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
    ) -> Mapping[str, Any] | Sequence[Mapping[str, Any]]:
        url = f"{self.base_url}{path}"
        response = self._session.request(method, url, headers=headers, params=params, json=json)
        response.raise_for_status()
        return response.json()

    def get_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Mapping[str, Any] | Sequence[Mapping[str, Any]]:
        return self.request("GET", endpoint, headers=headers, params=params)

    def paginate_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[Mapping[str, Any]]:
        del headers

        first_page = self.get_json(endpoint, params=params)
        pagination = PageParamPagination(page_param=page_param, page_key=page_key, next_key=next_key)
        yield from pagination.iter_pages(first_page, self, endpoint=endpoint, params=params)

    def close(self) -> None:
        self._session.close()


class AioHttpTransport(BaseApiClient):
    """Условный пример асинхронного транспорта на базе ``aiohttp``."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    async def _request_async(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
    ) -> Mapping[str, Any] | Sequence[Mapping[str, Any]]:
        try:
            import aiohttp
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("aiohttp is required for AioHttpTransport") from exc

        async with aiohttp.ClientSession() as session:
            async with session.request(
                method,
                f"{self.base_url}{path}",
                headers=headers,
                params=params,
                json=json,
            ) as response:
                response.raise_for_status()
                return await response.json()

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
    ) -> Mapping[str, Any] | Sequence[Mapping[str, Any]]:
        return asyncio.get_event_loop().run_until_complete(
            self._request_async(method, path, headers=headers, params=params, json=json)
        )

    def get_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Mapping[str, Any] | Sequence[Mapping[str, Any]]:
        return self.request("GET", endpoint, headers=headers, params=params)

    def paginate_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[Mapping[str, Any]]:
        del headers

        first_page = self.get_json(endpoint, params=params)
        pagination = PageParamPagination(page_param=page_param, page_key=page_key, next_key=next_key)
        yield from pagination.iter_pages(first_page, self, endpoint=endpoint, params=params)

    def close(self) -> None:
        return None


__all__ = ["RequestsTransport", "AioHttpTransport"]
