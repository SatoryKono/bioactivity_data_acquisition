"""Target schema module."""

from __future__ import annotations

from bioetl.schemas.target.target_chembl import (
    COLUMN_ORDER,
    SCHEMA_VERSION,
    TARGET_TYPES,
    TargetSchema,
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "TARGET_TYPES", "TargetSchema"]

