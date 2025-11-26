from __future__ import annotations

from collections.abc import Callable
from typing import Any, Callable

from bioetl.core.http.pagination import (
    NextLinkPagination,
    PageParamPagination,
    PaginationStrategy,
)


class PaginationRegistry:
    """Реестр фабрик стратегий пагинации."""

    def __init__(self) -> None:
        self._factories: dict[str, Callable[..., PaginationStrategy]] = {}

    def register(self, name: str, factory: Callable[..., PaginationStrategy]) -> None:
        """Зарегистрировать фабрику стратегии под указанным именем."""

        self._factories[name] = factory

    def create(self, name: str, **kwargs: Any) -> PaginationStrategy:
        """Создать экземпляр стратегии по имени."""

        try:
            factory = self._factories[name]
        except KeyError as exc:  # pragma: no cover - защитный случай
            available = ", ".join(sorted(self._factories)) or "<empty>"
            raise KeyError(f"Pagination strategy '{name}' is not registered. Available: {available}") from exc
        return factory(**kwargs)

    def __contains__(self, name: str) -> bool:
        return name in self._factories

    def available(self) -> tuple[str, ...]:
        return tuple(sorted(self._factories))


_default_registry: PaginationRegistry | None = None


def _build_next_link(**kwargs: Any) -> PaginationStrategy:
    return NextLinkPagination(**kwargs)


def _build_page_param(**kwargs: Any) -> PaginationStrategy:
    return PageParamPagination(**kwargs)


def get_default_pagination_registry() -> PaginationRegistry:
    """Лениво сконфигурированный реестр стратегий пагинации по умолчанию."""

    global _default_registry
    if _default_registry is None:
        registry = PaginationRegistry()
        registry.register("next_link", _build_next_link)
        registry.register("page_param", _build_page_param)
        _set_default_pagination_registry(registry)
    return _default_registry


def _set_default_pagination_registry(registry: PaginationRegistry) -> None:
    global _default_registry
    _default_registry = registry


__all__ = [
    "PaginationRegistry",
    "get_default_pagination_registry",
]
