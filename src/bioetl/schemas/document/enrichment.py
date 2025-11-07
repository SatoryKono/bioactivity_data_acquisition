"""Pandera schemas for document enrichment outputs."""

from __future__ import annotations

from pandera import DataFrameSchema

from bioetl.schemas.common import nullable_string_column

SCHEMA_VERSION = "1.0.0"

DOCUMENT_TERMS_ENRICHMENT_SCHEMA = DataFrameSchema(
    {
        "term": nullable_string_column(),
        "weight": nullable_string_column(),
    },
    strict=False,
    coerce=False,
    ordered=False,
    name=f"DocumentTermsEnrichment_v{SCHEMA_VERSION}",
)

__all__ = ["SCHEMA_VERSION", "DOCUMENT_TERMS_ENRICHMENT_SCHEMA"]

