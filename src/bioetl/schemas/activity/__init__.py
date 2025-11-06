"""Activity schema module."""

from __future__ import annotations

from bioetl.schemas.activity.activity_chembl import (
    ACTIVITY_PROPERTY_KEYS,
    ActivitySchema,
    COLUMN_ORDER,
    RELATIONS,
    SCHEMA_VERSION,
    STANDARD_TYPES,
)

__all__ = [
    "SCHEMA_VERSION",
    "COLUMN_ORDER",
    "STANDARD_TYPES",
    "RELATIONS",
    "ACTIVITY_PROPERTY_KEYS",
    "ActivitySchema",
]

