"""Deprecated compatibility layer for ChEMBL pagination helpers.

This module preserves historical import paths by re-exporting pagination
utilities from ``bioetl.clients.pagination`` while emitting a
:class:`DeprecationWarning`.
"""
from __future__ import annotations

import warnings

from bioetl.clients.pagination import (
    DEFAULT_PAGINATION_STRATEGY,
    PaginationFactory,
    PaginationStrategy,
    available_pagination_strategies,
    create_pagination_strategy,
    register_pagination_strategy,
)

warnings.warn(
    "Importing from 'bioetl.clients.chembl.pagination' is deprecated; "
    "use 'bioetl.clients.pagination' instead.",
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

