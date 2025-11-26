from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from typing import Any, Protocol, TypeVar

JSONPayload = Mapping[str, Any] | list[Mapping[str, Any]]
JSONPage = Iterator[Mapping[str, Any]]
JSONRecord = Mapping[str, Any]
JSONRecordStream = Iterator[JSONRecord]

_T = TypeVar("_T")


class WrapCallable(Protocol):
    def __call__(
        self, func: Callable[[], _T], log_context: Mapping[str, Any] | None = None
    ) -> _T:
        ...


class WrapIterator(Protocol):
    def __call__(
        self,
        func: Callable[[], Iterator[dict[str, Any]]],
        log_context: Mapping[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        ...


Normalizer = Callable[[Any], Iterator[dict[str, Any]]]


__all__ = [
    "JSONPayload",
    "JSONPage",
    "JSONRecord",
    "JSONRecordStream",
    "WrapCallable",
    "WrapIterator",
    "Normalizer",
]
