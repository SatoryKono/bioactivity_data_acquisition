from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from bioetl.core.http.pagination import (
    NextLinkPagination,
    PageParamPagination,
    PaginationStrategy,
)

PaginationFactory = Callable[..., PaginationStrategy]

DEFAULT_PAGINATION_STRATEGY = "next_link"

_PAGINATION_FACTORIES: dict[str, PaginationFactory] = {
    "next_link": NextLinkPagination,
    "page_param": PageParamPagination,
}


def register_pagination_strategy(name: str, factory: PaginationFactory) -> None:
    """Register a pagination strategy factory by name."""

    _PAGINATION_FACTORIES[name] = factory


def available_pagination_strategies() -> tuple[str, ...]:
    """Return sorted names of available pagination strategies."""

    return tuple(sorted(_PAGINATION_FACTORIES))


def create_pagination_strategy(
    name: str | None = None,
    *,
    factories: Mapping[str, PaginationFactory] | None = None,
    **kwargs: Any,
) -> PaginationStrategy:
    """Create a pagination strategy from registered factories."""

    strategy_name = name or DEFAULT_PAGINATION_STRATEGY
    registry: dict[str, PaginationFactory] = dict(_PAGINATION_FACTORIES)
    if factories:
        registry.update(factories)

    try:
        factory = registry[strategy_name]
    except KeyError as exc:  # pragma: no cover - safety guard
        available = ", ".join(sorted(registry)) or "<empty>"
        raise KeyError(
            f"Pagination strategy '{strategy_name}' is not registered. Available: {available}"
        ) from exc
    return factory(**kwargs)


__all__ = [
    "PaginationStrategy",
    "PaginationFactory",
    "DEFAULT_PAGINATION_STRATEGY",
    "available_pagination_strategies",
    "create_pagination_strategy",
    "register_pagination_strategy",
]
