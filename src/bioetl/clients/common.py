from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any, Protocol, TypeVar

from bioetl.base_classes import BaseApiClient
from bioetl.clients import client_exceptions


_T = TypeVar("_T")


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
    "ApiClientMixin",
    "ClosableMixin",
    "PaginationStrategy",
    "NextLinkPagination",
    "PageParamPagination",
]
