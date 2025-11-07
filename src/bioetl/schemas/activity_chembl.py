"""Backward-compatible import for activity schema."""

from __future__ import annotations

from bioetl.schemas.activity.activity_chembl import *  # noqa: F401,F403

__all__ = [
    "SCHEMA_VERSION",
    "COLUMN_ORDER",
    "STANDARD_TYPES",
    "RELATIONS",
    "ACTIVITY_PROPERTY_KEYS",
    "ActivitySchema",
]


