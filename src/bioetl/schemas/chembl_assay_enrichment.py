"""Pandera schemas for assay enrichment outputs."""

from __future__ import annotations

from bioetl.schemas.base_abstract_schema import create_schema
from bioetl.schemas.common_column_factory import nullable_string_column

SCHEMA_VERSION = "1.0.0"

ASSAY_CLASSIFICATION_ENRICHMENT_SCHEMA = create_schema(
    columns={
        "assay_classifications": nullable_string_column(),
        "assay_class_id": nullable_string_column(),
    },
    version=SCHEMA_VERSION,
    name="AssayClassificationEnrichment",
    ordered=False,
)

ASSAY_PARAMETERS_ENRICHMENT_SCHEMA = create_schema(
    columns={
        "assay_parameters": nullable_string_column(),
    },
    version=SCHEMA_VERSION,
    name="AssayParametersEnrichment",
    ordered=False,
)

__all__ = [
    "SCHEMA_VERSION",
    "ASSAY_CLASSIFICATION_ENRICHMENT_SCHEMA",
    "ASSAY_PARAMETERS_ENRICHMENT_SCHEMA",
]
