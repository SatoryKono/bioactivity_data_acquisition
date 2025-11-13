"""Совместимость: экспорт схемы целей под историческим пространством имён."""

from __future__ import annotations

from bioetl.schemas.chembl_target_schema import (
    COLUMN_ORDER,
    SCHEMA_VERSION,
    TARGET_TYPES,
    TargetSchema,
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "TARGET_TYPES", "TargetSchema"]


