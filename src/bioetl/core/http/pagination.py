from __future__ import annotations

from typing import Any, Callable, Iterator, Mapping, MutableMapping, Protocol, runtime_checkable


FetchPage = Callable[[str, Mapping[str, Any] | None], Mapping[str, Any]]


@runtime_checkable
class PaginationStrategy(Protocol):
    """Контракт для обхода страниц API."""

    def paginate(
        self,
        path: str,
        params: Mapping[str, Any],
        fetch: FetchPage,
        *,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[Mapping[str, Any]]:
        ...


class DefaultPaginationStrategy(PaginationStrategy):
    def paginate(
        self,
        path: str,
        params: Mapping[str, Any],
        fetch: FetchPage,
        *,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[Mapping[str, Any]]:
        next_path: str | None = path
        page_params: MutableMapping[str, Any] = dict(params)
        if page_param:
            page_params.setdefault(page_param, 1)

        while next_path:
            payload = fetch(next_path, page_params)
            yield payload

            if not isinstance(payload, Mapping):
                break

            next_candidate = payload.get(next_key)
            if isinstance(next_candidate, str):
                next_path = next_candidate
                page_params = {}
                continue

            items = payload.get(page_key)
            if items and page_param:
                page_params[page_param] = page_params.get(page_param, 1) + 1
                next_path = path
            else:
                break


__all__ = ["DefaultPaginationStrategy", "PaginationStrategy", "FetchPage"]
