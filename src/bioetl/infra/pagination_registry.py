from __future__ import annotations

from collections.abc import Callable
from typing import Protocol


class PaginationStrategy(Protocol):
    def paginate(self, *args, **kwargs):  # pragma: no cover - protocol definition
        ...


class PaginationRegistry:
    """Реестр фабрик стратегий пагинации с доступом по имени."""

    def __init__(
        self,
        initial: dict[str, Callable[[], PaginationStrategy]] | None = None,
    ) -> None:
        self._factories: dict[str, Callable[[], PaginationStrategy]] = {}
        if initial:
            for name, factory in initial.items():
                self.register(name, factory)

    def register(self, name: str, factory: Callable[[], PaginationStrategy]) -> None:
        key = name.lower().strip()
        self._factories[key] = factory

    def get(self, name: str) -> PaginationStrategy:
        key = name.lower().strip()
        if key not in self._factories:
            available = ", ".join(sorted(self._factories))
            raise KeyError(f"Pagination strategy '{name}' is not registered; available: {available}")
        return self._factories[key]()

    def available(self) -> list[str]:
        return sorted(self._factories)


default_pagination_registry = PaginationRegistry()


__all__ = ["PaginationRegistry", "default_pagination_registry"]
