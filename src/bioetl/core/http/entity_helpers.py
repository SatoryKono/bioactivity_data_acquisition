from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, TypeVar

from bioetl.core.http.pagination import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
    PaginationStrategy,
)
from bioetl.core.http.types import Normalizer, WrapCallable, WrapIterator

_T = TypeVar("_T")


def iterate_entity_records(
    *,
    ids: Sequence[str] | None,
    page_size: int | None,
    fetcher: Callable[[Sequence[str] | None], _T] | None,
    fetch_by_ids: Callable[[Sequence[str]], Iterator[dict[str, Any]]],
    list_entities: Callable[[], Iterator[dict[str, Any]]],
    normalize_payload: Normalizer,
    wrap_iterator: WrapIterator,
) -> Iterator[dict[str, Any]]:
    """Унифицированный обход записей сущности с поддержкой ``ids`` и кастомного fetcher."""

    def iterator() -> Iterator[dict[str, Any]]:
        if callable(fetcher):
            result = fetcher(ids)
            if isinstance(result, Iterator):
                yield from result
                return
            if result is not None:
                yield from normalize_payload(result)
                return

        if ids:
            yield from fetch_by_ids(ids)
            return

        yield from list_entities()

    return wrap_iterator(iterator, log_context=None)


def list_entities(
    *,
    transport: Any,
    entity_path: str,
    pagination_strategy: PaginationStrategy,
    wrap_callable: WrapCallable,
    wrap_iterator: WrapIterator,
    normalize_payload: Normalizer,
    normalize_page: Normalizer,
    logger: Any,
    page_size: int = 1000,
    params: Mapping[str, Any] | None = None,
    page_key: str = DEFAULT_PAGE_KEY,
    next_key: str = DEFAULT_NEXT_KEY,
    page_param: str | None = DEFAULT_PAGE_PARAM,
) -> Iterator[dict[str, Any]]:
    """Общий помощник для обхода страниц сущности через стратегию пагинации."""

    def iterator() -> Iterator[dict[str, Any]]:
        query_params: dict[str, Any] = {"limit": page_size}
        if params:
            query_params.update(params)

        first_payload = wrap_callable(
            lambda: transport.request("GET", entity_path, params=query_params),
            log_context={"path": entity_path},
        )
        logger.info("api_call", path=entity_path)

        for page in pagination_strategy.iter_pages(
            first_payload,
            transport,
            endpoint=entity_path,
            params=query_params,
            logger=logger,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
            normalize=normalize_payload,
        ):
            yield from normalize_page(page)

    return wrap_iterator(iterator, log_context={"path": entity_path})


def fetch_all_entities(
    *,
    list_entities_fn: Callable[[], Iterator[dict[str, Any]]],
    wrap_iterator: WrapIterator,
) -> Iterator[dict[str, Any]]:
    """Совместимая обёртка ``fetch_all`` для перечисления всех сущностей."""

    return wrap_iterator(list_entities_fn, log_context=None)


__all__ = [
    "DEFAULT_NEXT_KEY",
    "DEFAULT_PAGE_KEY",
    "DEFAULT_PAGE_PARAM",
    "fetch_all_entities",
    "iterate_entity_records",
    "list_entities",
]
