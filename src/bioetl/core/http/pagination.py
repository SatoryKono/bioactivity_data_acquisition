"""HTTP pagination strategies and utilities."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from bioetl.core.http.interfaces import ApiTransportProtocol
else:  # pragma: no cover - runtime-only Protocol fallback
    class ApiTransportProtocol(Protocol):
        def request(
            self,
            method: str,
            path: str,
            *,
            headers: Mapping[str, str] | None = None,
            params: Mapping[str, Any] | None = None,
            json: Any | None = None,
        ) -> Mapping[str, Any] | Sequence[Mapping[str, Any]]:
            ...

        def close(self) -> None:
            ...

ResponsePayload = Mapping[str, Any] | Sequence[Mapping[str, Any]]

DEFAULT_PAGE_KEY = "results"
DEFAULT_NEXT_KEY = "next"
DEFAULT_PAGE_PARAM = "page"


@runtime_checkable
class PaginationStrategy(Protocol):
    """Контракт для обхода страниц API на уровне транспорта."""

    def iter_pages(
        self,
        initial_response: ResponsePayload,
        transport: ApiTransportProtocol,
        *,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        logger: Any | None = None,
        page_key: str | None = "results",
        next_key: str | None = "next",
        page_param: str | None = "page",
        normalize: Any | None = None,
    ) -> Iterator[ResponsePayload]:
        ...


class DefaultPaginationStrategy(PaginationStrategy):
    """Простая стратегия пагинации, использующая номер страницы."""

    def __init__(
        self,
        *,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> None:
        self.page_key = page_key
        self.next_key = next_key
        self.page_param = page_param

    def iter_pages(
        self,
        initial_response: ResponsePayload,
        transport: ApiTransportProtocol,
        *,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        logger: Any | None = None,
        page_key: str | None = None,
        next_key: str | None = None,
        page_param: str | None = None,
        normalize: Any | None = None,
    ) -> Iterator[ResponsePayload]:
        pagination = PageParamPagination(
            page_param=self.page_param,
            page_key=self.page_key,
            next_key=self.next_key,
        )
        yield from pagination.iter_pages(
            initial_response,
            transport,
            endpoint=endpoint,
            params=params,
            logger=logger,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
            normalize=normalize,
        )


class NextLinkPagination:
    """Paginate using 'next' URL from response payload."""

    def __init__(
        self,
        *,
        page_key: str = DEFAULT_NEXT_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
    ) -> None:
        """Initialize pagination strategy."""
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
        normalize: Any | None = None,
    ) -> Iterator[Mapping[str, Any] | Sequence[Mapping[str, Any]]]:
        """Iterate through paginated responses using next links."""
        del page_param, normalize

        page_key = page_key or self.page_key
        next_key = next_key or self.next_key
        next_path = endpoint
        response: (
            Mapping[str, Any] | Sequence[Mapping[str, Any]]
        ) = initial_response
        query_params: Mapping[str, Any] | None = (
            dict(params) if params else None
        )

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
        """Initialize page parameter pagination strategy."""
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
        normalize: Any | None = None,
    ) -> Iterator[Mapping[str, Any] | Sequence[Mapping[str, Any]]]:
        """Iterate through paginated responses using page parameters."""
        page_key = page_key or self.page_key
        next_key = next_key or self.next_key
        page_param = (
            page_param if page_param is not None else self.page_param
        )

        page_num = 1
        next_path = endpoint
        query_params = dict(params) if params else {}
        response: (
            Mapping[str, Any] | Sequence[Mapping[str, Any]]
        ) = initial_response

        while next_path:
            yield response

            next_candidate = (
                response.get(next_key) if isinstance(response, Mapping) else None
            )
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

            page_items = list(
                _iter_payload_items(
                    response, page_key=page_key, normalize=normalize
                )
            )
            if not page_items:
                break

            page_num += 1
            effective_params = dict(query_params)
            if page_param is not None:
                effective_params[page_param] = page_num
            response = transport.request("GET", next_path, params=effective_params)
            if logger:
                logger.info("api_call", path=next_path)


def _iter_payload_items(
    payload: Any, *, page_key: str, normalize: Any | None
) -> Iterator[dict[str, Any]]:
    if normalize is not None:
        yield from normalize(payload)
        return

    if isinstance(payload, Mapping):
        items = payload.get(page_key)
        if isinstance(items, list):
            yield from items
        return

    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        yield from payload
        return

    if payload:
        yield payload


__all__ = [
    "DEFAULT_NEXT_KEY",
    "DEFAULT_PAGE_KEY",
    "DEFAULT_PAGE_PARAM",
    "DefaultPaginationStrategy",
    "NextLinkPagination",
    "PageParamPagination",
    "PaginationStrategy",
    "ResponsePayload",
]
