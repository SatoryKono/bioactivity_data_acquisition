"""Общие утилиты для клиентских модулей."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from typing import Any, Callable, TypeVar

import backoff

T = TypeVar("T")
U = TypeVar("U")


def safe_cast(value: Any, target_type: Callable[[Any], U] | type[U], default: U | None = None) -> U | None:
    """Безопасно привести значение к заданному типу.

    При ошибке преобразования возвращается ``default`` (по умолчанию ``None``).
    """

    if value is None:
        return default
    try:
        return target_type(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def flatten_dict(mapping: Mapping[str, Any], *, parent_key: str = "", sep: str = ".") -> dict[str, Any]:
    """Свести вложенный словарь к плоскому представлению с разделителем ``sep``."""

    items: dict[str, Any] = {}
    for key, value in mapping.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, Mapping):
            items.update(flatten_dict(value, parent_key=new_key, sep=sep))
        else:
            items[new_key] = value
    return items


def batch_iterator(iterable: Iterable[T], batch_size: int) -> Iterator[tuple[T, ...]]:
    """Итерировать по последовательности батчами фиксированного размера."""

    if batch_size <= 0:
        raise ValueError("batch_size должен быть положительным")

    batch: list[T] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield tuple(batch)
            batch.clear()
    if batch:
        yield tuple(batch)


def retry_backoff(
    exc_types: tuple[type[BaseException], ...] = (Exception,),
    *,
    max_tries: int = 3,
    factor: int | float = 2,
    base: float = 0.5,
):
    """Декоратор экспоненциального backoff для повторов HTTP-запросов."""

    return backoff.on_exception(backoff.expo, exc_types, max_tries=max_tries, factor=factor, base=base)
