from __future__ import annotations

"""Утилиты пагинации для клиентского уровня."""

from dataclasses import dataclass
from typing import Any, Iterable, Iterator

from bioetl.clients.base.types import Record


@dataclass(slots=True)
class PaginationParams:
    page_param: str | None = None
    page_size_param: str | None = None
    cursor_param: str | None = None
    limit_param: str | None = None
    offset_param: str | None = None
    page_size: int | None = None
    max_pages: int | None = None

    def override(self, **kwargs: Any) -> "PaginationParams":
        data = self.__dict__ | {k: v for k, v in kwargs.items() if v is not None}
        return PaginationParams(**data)


@dataclass(slots=True)
class Page:
    items: list[Record]
    next_cursor: str | int | None = None
    has_next: bool = False
    raw: Any | None = None


def ensure_pages(records: Iterable[Record]) -> Iterator[Page]:
    for record in records:
        yield Page(items=[record], has_next=True)


__all__ = ["Page", "PaginationParams", "ensure_pages"]
