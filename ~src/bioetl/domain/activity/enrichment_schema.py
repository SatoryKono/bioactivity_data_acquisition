"""Pandera schemas for activity enrichment outputs."""

from __future__ import annotations

from bioetl.schemas.base_abstract_schema import create_schema
from bioetl.schemas.common_column_factory import SchemaColumnFactory

SCHEMA_VERSION = "1.0.0"
CF = SchemaColumnFactory

ASSAY_ENRICHMENT_SCHEMA = create_schema(
    columns={
        "assay_organism": CF.string(),
        "assay_tax_id": CF.int64(),
    },
    version=SCHEMA_VERSION,
    name="ActivityAssayEnrichment",
    ordered=False,
)

COMPOUND_RECORD_ENRICHMENT_SCHEMA = create_schema(
    columns={
        "compound_name": CF.string(),
        "compound_key": CF.string(),
        "curated": CF.boolean_flag(),
        "removed": CF.boolean_flag(),
    },
    version=SCHEMA_VERSION,
    name="ActivityCompoundEnrichment",
    ordered=False,
)

DATA_VALIDITY_ENRICHMENT_SCHEMA = create_schema(
    columns={
        "data_validity_description": CF.string(),
    },
    version=SCHEMA_VERSION,
    name="ActivityDataValidityEnrichment",
    ordered=False,
)

__all__ = [
    "SCHEMA_VERSION",
    "ASSAY_ENRICHMENT_SCHEMA",
    "COMPOUND_RECORD_ENRICHMENT_SCHEMA",
    "DATA_VALIDITY_ENRICHMENT_SCHEMA",
]
