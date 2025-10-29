"""Compatibility layer for legacy dtype helpers.

The project migrated helpers to :mod:`bioetl.utils.dtypes`.  This module keeps
old import paths alive while delegating to the new implementations.
"""

from __future__ import annotations

from typing import Any
from warnings import warn

from bioetl.utils.dtypes import (
    NAType,
    coerce_nullable_float,
    coerce_nullable_float_columns,
    coerce_nullable_int,
    coerce_nullable_int_columns,
    coerce_optional_bool,
    coerce_retry_after,
)

__all__ = [
    "NAType",
    "coerce_nullable_float",
    "coerce_nullable_float_columns",
    "coerce_nullable_int",
    "coerce_nullable_int_columns",
    "coerce_optional_bool",
    "coerce_retry_after",
]


def __getattr__(name: str) -> Any:  # pragma: no cover - compatibility shim
    if name in __all__:
        warn(
            "Importing from 'bioetl.utils.dtype' is deprecated; "
            "use 'bioetl.utils.dtypes' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return globals()[name]
    raise AttributeError(f"module 'bioetl.utils.dtype' has no attribute '{name}'")
