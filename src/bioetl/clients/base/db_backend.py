from __future__ import annotations

"""Контракты для DB-бэкендов."""

from typing import Protocol

from bioetl.clients.base.types import Record


class DbBackend(Protocol):
    def fetch_one(self, query: str, *, params: dict[str, object] | None = None) -> Record | None:
        ...

    def fetch_many(self, query: str, *, params: dict[str, object] | None = None) -> list[Record]:
        ...

    def close(self) -> None:
        ...


__all__ = ["DbBackend"]
