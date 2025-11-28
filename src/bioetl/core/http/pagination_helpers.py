from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any, TypeVar
import warnings

from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
    PaginationStrategy,
)
from bioetl.core.http.types import Normalizer, WrapCallable, WrapIterator

_T = TypeVar("_T")


def normalize_payload(payload: Any, *, page_key: str | None = DEFAULT_PAGE_KEY) -> Iterator[dict[str, Any]]:
    """Привести произвольный ответ API к итератору словарей."""

    effective_page_key = page_key if page_key is not None else DEFAULT_PAGE_KEY

    if isinstance(payload, Mapping):
        results = payload.get(effective_page_key)
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


def iter_pages(
    strategy: PaginationStrategy,
    first_payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    transport: ApiTransportProtocol,
    *,
    endpoint: str,
    params: Mapping[str, Any] | None,
    logger: Any | None,
    page_key: str | None,
    next_key: str | None,
    page_param: str | None,
    normalize: Normalizer | None,
) -> Iterator[Mapping[str, Any] | Sequence[Mapping[str, Any]]]:
    """Обёртка над ``PaginationStrategy.iter_pages`` с передачей контекста."""

    yield from strategy.iter_pages(
        first_payload,
        transport,
        endpoint=endpoint,
        params=params,
        logger=logger,
        page_key=page_key,
        next_key=next_key,
        page_param=page_param,
        normalize=normalize,
    )


def iter_ids(
    *,
    ids: Sequence[str],
    entity: str,
    transport: ApiTransportProtocol,
    normalize: Normalizer,
    wrap_callable: WrapCallable,
    wrap_iterator: WrapIterator,
    logger: Any,
    path_template: str = "/{entity}/{id}",
    params: Mapping[str, Any] | None = None,
) -> Iterator[dict[str, Any]]:
    """Унифицированный обход сущностей по списку идентификаторов."""

    def iterator() -> Iterator[dict[str, Any]]:
        for raw_id in ids:
            entity_id = str(raw_id)
            path = path_template.format(entity=entity, id=entity_id)
            payload = wrap_callable(
                lambda: transport.request("GET", path, params=params),
                log_context={"path": path},
            )
            logger.info("api_call", entity=entity, entity_id=entity_id)
            yield from normalize(payload)

    return wrap_iterator(iterator, log_context=None)


def iterate_records(
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
    """Совместимый алиас ``iterate_records`` для обхода записей сущности."""

    return iterate_records(
        ids=ids,
        page_size=page_size,
        fetcher=fetcher,
        fetch_by_ids=fetch_by_ids,
        list_entities=list_entities,
        normalize_payload=normalize_payload,
        wrap_iterator=wrap_iterator,
    )


def list_entities(
    *,
    transport: ApiTransportProtocol,
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

        for page in iter_pages(
            pagination_strategy,
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


def warn_fetch_all(
    *,
    list_entities_fn: Callable[[], Iterator[dict[str, Any]]],
    wrap_iterator: WrapIterator,
) -> Iterator[dict[str, Any]]:
    """Совместимая обёртка ``fetch_all`` для перечисления всех сущностей."""

    warnings.warn(
        "fetch_all is deprecated; use list instead to enumerate entities.",
        DeprecationWarning,
        stacklevel=2,
    )
    return wrap_iterator(list_entities_fn, log_context=None)


def fetch_all_entities(
    *,
    list_entities_fn: Callable[[], Iterator[dict[str, Any]]],
    wrap_iterator: WrapIterator,
) -> Iterator[dict[str, Any]]:
    """Алиас ``warn_fetch_all`` для обратной совместимости с клиентами."""

    return warn_fetch_all(list_entities_fn=list_entities_fn, wrap_iterator=wrap_iterator)


__all__ = [
    "DEFAULT_NEXT_KEY",
    "DEFAULT_PAGE_KEY",
    "DEFAULT_PAGE_PARAM",
    "PaginationStrategy",
    "fetch_all_entities",
    "iter_ids",
    "iter_pages",
    "iterate_entity_records",
    "iterate_records",
    "list_entities",
    "normalize_payload",
    "warn_fetch_all",
]
