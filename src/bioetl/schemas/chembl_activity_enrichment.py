"""Pandera schemas for activity enrichment outputs."""

from __future__ import annotations

from bioetl.schemas.base_abstract_schema import create_schema
from bioetl.schemas.common_column_factory import (
    boolean_flag_column,
    nullable_int64_column,
    nullable_string_column,
)

SCHEMA_VERSION = "1.0.0"

ASSAY_ENRICHMENT_SCHEMA = create_schema(
    columns={
        "assay_organism": nullable_string_column(),
        "assay_tax_id": nullable_int64_column(),
    },
    version=SCHEMA_VERSION,
    name="ActivityAssayEnrichment",
    ordered=False,
)

COMPOUND_RECORD_ENRICHMENT_SCHEMA = create_schema(
    columns={
        "compound_name": nullable_string_column(),
        "compound_key": nullable_string_column(),
        "curated": boolean_flag_column(),
        "removed": boolean_flag_column(),
    },
    version=SCHEMA_VERSION,
    name="ActivityCompoundEnrichment",
    ordered=False,
)

DATA_VALIDITY_ENRICHMENT_SCHEMA = create_schema(
    columns={
        "data_validity_description": nullable_string_column(),
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
