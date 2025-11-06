"""Assay schema module."""

from __future__ import annotations

from bioetl.schemas.assay.assay_chembl import (
    AssaySchema,
    COLUMN_ORDER,
    SCHEMA_VERSION,
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "AssaySchema"]

