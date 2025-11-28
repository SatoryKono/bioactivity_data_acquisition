"""Deprecated compatibility layer for pagination utilities."""
from __future__ import annotations

import warnings

from bioetl.clients.chembl.pagination import (
    DEFAULT_PAGINATION_STRATEGY,
    PaginationFactory,
    PaginationStrategy,
    available_pagination_strategies,
    create_pagination_strategy,
    register_pagination_strategy,
)

warnings.warn(
    "'bioetl.clients.pagination' is deprecated; use "
    "'bioetl.clients.chembl.pagination' instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "PaginationStrategy",
    "PaginationFactory",
    "DEFAULT_PAGINATION_STRATEGY",
    "available_pagination_strategies",
    "create_pagination_strategy",
    "register_pagination_strategy",
]
