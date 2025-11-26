from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any
import warnings

from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import PaginationStrategy


DEFAULT_PAGE_KEY = "results"
DEFAULT_NEXT_KEY = "next"
DEFAULT_PAGE_PARAM = "page"


def normalize_payload(payload: Any, page_key: str | None = DEFAULT_PAGE_KEY) -> Iterator[dict[str, Any]]:
    """Приводит полезную нагрузку ответа к итератору словарей."""

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
    pagination_strategy: PaginationStrategy,
    first_payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    transport: ApiTransportProtocol,
    *,
    endpoint: str,
    params: Mapping[str, Any] | None = None,
    logger: Any | None = None,
    page_key: str | None = DEFAULT_PAGE_KEY,
    next_key: str | None = DEFAULT_NEXT_KEY,
    page_param: str | None = DEFAULT_PAGE_PARAM,
    normalize: Callable[[Any, str | None], Iterator[dict[str, Any]]] | None = None,
) -> Iterator[Mapping[str, Any] | Sequence[Mapping[str, Any]]]:
    """Обход страниц с помощью выбранной стратегии пагинации."""

    yield from pagination_strategy.iter_pages(
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
    normalize: Callable[[Any, str | None], Iterator[dict[str, Any]]],
    wrap_callable: Callable[[Callable[[], Any], Mapping[str, Any] | None], Any],
    wrap_iterator: Callable[[Callable[[], Iterator[dict[str, Any]]], Mapping[str, Any] | None], Iterator[dict[str, Any]]],
    logger: Any,
    path_template: str = "/{entity}/{id}",
) -> Iterator[dict[str, Any]]:
    """Получение сущностей по идентификаторам с нормализацией ответа."""

    def iterator() -> Iterator[dict[str, Any]]:
        for raw_id in ids:
            entity_id = str(raw_id)
            path = path_template.format(entity=entity, id=entity_id)
            payload = wrap_callable(lambda: transport.request("GET", path), log_context={"path": path})
            logger.info("api_call", entity=entity, entity_id=entity_id)
            yield from normalize(payload, page_key=None)

    return wrap_iterator(iterator)


def iterate_records(
    *,
    ids: Sequence[str] | None,
    page_size: int | None,
    fetcher: Callable[[Sequence[str] | None], Any] | None,
    fetch_by_ids: Callable[[Sequence[str]], Iterator[dict[str, Any]]],
    list_entities: Callable[[int | None], Iterator[dict[str, Any]]],
    normalize: Callable[[Any, str | None], Iterator[dict[str, Any]]],
    wrap_iterator: Callable[[Callable[[], Iterator[dict[str, Any]]], Mapping[str, Any] | None], Iterator[dict[str, Any]]],
) -> Iterator[dict[str, Any]]:
    """Единая логика обхода записей для разных сценариев выборки."""

    def iterator() -> Iterator[dict[str, Any]]:
        if callable(fetcher):
            result = fetcher(ids)
            if isinstance(result, Iterator):
                yield from result
                return
            if result is not None:
                yield from normalize(result, page_key=None)
                return

        if ids:
            yield from fetch_by_ids(ids)
            return

        yield from list_entities(page_size)

    return wrap_iterator(iterator)


def list_entities(
    *,
    page_size: int,
    params: Mapping[str, Any] | None,
    pagination_strategy: PaginationStrategy,
    endpoint: str,
    transport: ApiTransportProtocol,
    logger: Any | None,
    page_key: str,
    next_key: str,
    page_param: str | None,
    normalize: Callable[[Any, str | None], Iterator[dict[str, Any]]],
    wrap_callable: Callable[[Callable[[], Any], Mapping[str, Any] | None], Any],
    wrap_iterator: Callable[[Callable[[], Iterator[dict[str, Any]]], Mapping[str, Any] | None], Iterator[dict[str, Any]]],
) -> Iterator[dict[str, Any]]:
    """Стандартный обход сущностей с использованием пагинации."""

    def iterator() -> Iterator[dict[str, Any]]:
        query_params: dict[str, Any] = {"limit": page_size}
        if params:
            query_params.update(params)

        first_payload = wrap_callable(
            lambda: transport.request("GET", endpoint, params=query_params),
            log_context={"path": endpoint},
        )

        if logger:
            logger.info("api_call", path=endpoint)

        for page in iter_pages(
            pagination_strategy,
            first_payload,
            transport,
            endpoint=endpoint,
            params=query_params,
            logger=logger,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
            normalize=normalize,
        ):
            yield from normalize(page, page_key=page_key)

    return wrap_iterator(iterator)


def warn_fetch_all(
    *,
    list_callable: Callable[..., Iterator[dict[str, Any]]],
    list_kwargs: Mapping[str, Any],
) -> Iterator[dict[str, Any]]:
    """Вывод предупреждения о деприкации fetch_all и делегирование в list."""

    warnings.warn(
        "fetch_all is deprecated; use list instead to enumerate entities.",
        DeprecationWarning,
        stacklevel=2,
    )
    yield from list_callable(**list_kwargs)


__all__ = [
    "DEFAULT_PAGE_KEY",
    "DEFAULT_NEXT_KEY",
    "DEFAULT_PAGE_PARAM",
    "iter_pages",
    "iter_ids",
    "iterate_records",
    "list_entities",
    "normalize_payload",
    "warn_fetch_all",
]
