"""Pandera schemas for document enrichment outputs."""

from __future__ import annotations

from bioetl.schemas.base_abstract_schema import create_schema
from bioetl.schemas.common_column_factory import nullable_string_column

SCHEMA_VERSION = "1.0.0"

DOCUMENT_TERMS_ENRICHMENT_SCHEMA = create_schema(
    columns={
        "term": nullable_string_column(),
        "weight": nullable_string_column(),
    },
    version=SCHEMA_VERSION,
    name="DocumentTermsEnrichment",
    ordered=False,
)

__all__ = ["SCHEMA_VERSION", "DOCUMENT_TERMS_ENRICHMENT_SCHEMA"]
