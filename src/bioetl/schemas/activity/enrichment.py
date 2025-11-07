"""Pandera schemas for activity enrichment outputs."""

from __future__ import annotations

from pandera import DataFrameSchema

from bioetl.schemas.common import boolean_flag_column, nullable_int64_column, nullable_string_column

SCHEMA_VERSION = "1.0.0"

ASSAY_ENRICHMENT_SCHEMA = DataFrameSchema(
    {
        "assay_organism": nullable_string_column(),
        "assay_tax_id": nullable_int64_column(),
    },
    strict=False,
    coerce=False,
    ordered=False,
    name=f"ActivityAssayEnrichment_v{SCHEMA_VERSION}",
)

COMPOUND_RECORD_ENRICHMENT_SCHEMA = DataFrameSchema(
    {
        "compound_name": nullable_string_column(),
        "compound_key": nullable_string_column(),
        "curated": boolean_flag_column(),
        "removed": boolean_flag_column(),
    },
    strict=False,
    coerce=False,
    ordered=False,
    name=f"ActivityCompoundEnrichment_v{SCHEMA_VERSION}",
)

DATA_VALIDITY_ENRICHMENT_SCHEMA = DataFrameSchema(
    {
        "data_validity_description": nullable_string_column(),
    },
    strict=False,
    coerce=False,
    ordered=False,
    name=f"ActivityDataValidityEnrichment_v{SCHEMA_VERSION}",
)

__all__ = [
    "SCHEMA_VERSION",
    "ASSAY_ENRICHMENT_SCHEMA",
    "COMPOUND_RECORD_ENRICHMENT_SCHEMA",
    "DATA_VALIDITY_ENRICHMENT_SCHEMA",
]
