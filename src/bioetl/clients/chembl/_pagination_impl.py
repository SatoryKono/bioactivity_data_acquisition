from __future__ import annotations

import warnings

from bioetl.clients.chembl.pagination import (
    DEFAULT_PAGINATION_STRATEGY,
    PAGINATION_DEPRECATION_MESSAGE,
    PaginationFactory,
    PaginationStrategy,
    available_pagination_strategies,
    create_pagination_strategy,
    register_pagination_strategy,
)


warnings.warn(PAGINATION_DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)

__all__ = (
    "PaginationStrategy",
    "PaginationFactory",
    "DEFAULT_PAGINATION_STRATEGY",
    "available_pagination_strategies",
    "create_pagination_strategy",
    "register_pagination_strategy",
    "PAGINATION_DEPRECATION_MESSAGE",
)
