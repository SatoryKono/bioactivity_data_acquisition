"""Pandera schemas for document enrichment outputs."""

from __future__ import annotations

from bioetl.schemas.base_abstract_schema import create_schema
from bioetl.schemas.common_column_factory import SchemaColumnFactory

SCHEMA_VERSION = "1.0.0"
CF = SchemaColumnFactory

DOCUMENT_TERMS_ENRICHMENT_SCHEMA = create_schema(
    columns={
        "term": CF.string(),
        "weight": CF.string(),
    },
    version=SCHEMA_VERSION,
    name="DocumentTermsEnrichment",
    ordered=False,
)

__all__ = ["SCHEMA_VERSION", "DOCUMENT_TERMS_ENRICHMENT_SCHEMA"]
