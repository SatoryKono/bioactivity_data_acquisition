from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from bioetl.clients.common import PaginationStrategy


class PaginationRegistry:
    """Реестр фабрик стратегий пагинации."""

    def __init__(self) -> None:
        self._factories: dict[str, Callable[[], "PaginationStrategy"]] = {}

    def register(self, name: str, factory: Callable[[], "PaginationStrategy"]) -> None:
        self._factories[name] = factory

    def get(self, name: str) -> "PaginationStrategy":
        try:
            factory = self._factories[name]
        except KeyError as exc:  # pragma: no cover - защитный блок
            raise KeyError(f"Pagination strategy '{name}' is not registered") from exc
        return factory()

    def available(self) -> tuple[str, ...]:
        return tuple(self._factories.keys())


def create_default_pagination_registry() -> PaginationRegistry:
    from bioetl.clients.common import NextLinkPagination, PageParamPagination

    registry = PaginationRegistry()
    registry.register("next_link", NextLinkPagination)
    registry.register("page_param", PageParamPagination)
    return registry


default_pagination_registry = create_default_pagination_registry()

__all__ = ["PaginationRegistry", "create_default_pagination_registry", "default_pagination_registry"]
