from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from bioetl.clients.common import ApiTransportProtocol
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
        from bioetl.clients.common import PageParamPagination

        pagination = PageParamPagination(
            page_param=page_param,
            page_key=page_key or "results",
            next_key=next_key or "next",
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


__all__ = ["DefaultPaginationStrategy", "PaginationStrategy", "ResponsePayload"]
