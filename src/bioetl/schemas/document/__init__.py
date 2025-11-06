"""Document schema module."""

from __future__ import annotations

from bioetl.schemas.document.document_chembl import (
    COLUMN_ORDER,
    DocumentSchema,
    SCHEMA_VERSION,
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "DocumentSchema"]

