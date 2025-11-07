"""Pandera schemas for assay enrichment outputs."""

from __future__ import annotations

from pandera import DataFrameSchema

from bioetl.schemas.common import nullable_string_column

SCHEMA_VERSION = "1.0.0"

ASSAY_CLASSIFICATION_ENRICHMENT_SCHEMA = DataFrameSchema(
    {
        "assay_classifications": nullable_string_column(),
        "assay_class_id": nullable_string_column(),
    },
    strict=False,
    coerce=False,
    ordered=False,
    name=f"AssayClassificationEnrichment_v{SCHEMA_VERSION}",
)

ASSAY_PARAMETERS_ENRICHMENT_SCHEMA = DataFrameSchema(
    {
        "assay_parameters": nullable_string_column(),
    },
    strict=False,
    coerce=False,
    ordered=False,
    name=f"AssayParametersEnrichment_v{SCHEMA_VERSION}",
)

__all__ = [
    "SCHEMA_VERSION",
    "ASSAY_CLASSIFICATION_ENRICHMENT_SCHEMA",
    "ASSAY_PARAMETERS_ENRICHMENT_SCHEMA",
]

