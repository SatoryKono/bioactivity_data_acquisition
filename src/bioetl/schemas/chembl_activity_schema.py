"""Compatibility wrapper for the canonical activity schema module."""
from __future__ import annotations

from bioetl.schemas.activity import (
    ACTIVITY_PROPERTY_KEYS,
    BUSINESS_KEY_FIELDS,
    COLUMN_ORDER,
    RELATIONS,
    REQUIRED_FIELDS,
    ROW_HASH_FIELDS,
    SCHEMA_VERSION,
    ActivitySchema,
)

__all__ = [
    "SCHEMA_VERSION",
    "COLUMN_ORDER",
    "REQUIRED_FIELDS",
    "BUSINESS_KEY_FIELDS",
    "ROW_HASH_FIELDS",
    "RELATIONS",
    "ACTIVITY_PROPERTY_KEYS",
    "ActivitySchema",
]


