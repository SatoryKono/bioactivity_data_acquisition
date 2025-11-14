"""Pandera schemas for assay enrichment outputs."""

from __future__ import annotations

from bioetl.schemas.base_abstract_schema import create_schema
from bioetl.schemas.common_column_factory import SchemaColumnFactory

SCHEMA_VERSION = "1.0.0"
CF = SchemaColumnFactory

ASSAY_CLASSIFICATION_ENRICHMENT_SCHEMA = create_schema(
    columns={
        "assay_classifications": CF.string(),
        "assay_class_id": CF.string(),
    },
    version=SCHEMA_VERSION,
    name="AssayClassificationEnrichment",
    ordered=False,
)

ASSAY_PARAMETERS_ENRICHMENT_SCHEMA = create_schema(
    columns={
        "assay_parameters": CF.string(),
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
