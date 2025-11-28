from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from bioetl.core.http.pagination import DefaultPaginationStrategy, PaginationStrategy

PaginationFactory = Callable[[], PaginationStrategy]
DEFAULT_PAGINATION_STRATEGY = "default"


def register_pagination_strategy(
    name: str, factory: PaginationFactory, *, registry: dict[str, PaginationFactory] | None = None
) -> None:
    target = registry if registry is not None else _PAGINATION_REGISTRY
    target[name] = factory


def available_pagination_strategies(
    *, registry: Mapping[str, PaginationFactory] | None = None
) -> tuple[str, ...]:
    target = registry if registry is not None else _PAGINATION_REGISTRY
    return tuple(target.keys())


def create_pagination_strategy(
    name: str | None = None,
    *,
    factories: Mapping[str, PaginationFactory] | None = None,
    default: PaginationStrategy | None = None,
) -> PaginationStrategy:
    registry: dict[str, PaginationFactory] = dict(_PAGINATION_REGISTRY)
    if factories:
        registry.update(factories)

    strategy_name = name or DEFAULT_PAGINATION_STRATEGY
    if default is not None and strategy_name not in registry:
        return default

    factory = registry.get(strategy_name)
    if factory is None:
        raise KeyError(
            f"Unknown pagination strategy '{strategy_name}'. "
            f"Available: {', '.join(sorted(registry)) or 'none'}"
        )
    return factory()


_PAGINATION_REGISTRY: dict[str, PaginationFactory] = {}
register_pagination_strategy(DEFAULT_PAGINATION_STRATEGY, DefaultPaginationStrategy)

__all__ = [
    "PaginationStrategy",
    "PaginationFactory",
    "DEFAULT_PAGINATION_STRATEGY",
    "available_pagination_strategies",
    "create_pagination_strategy",
    "register_pagination_strategy",
]
