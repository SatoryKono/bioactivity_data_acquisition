"""Pandera schema describing the normalized ChEMBL assay dataset."""

from __future__ import annotations

import pandera as pa
from pandera import Column

from ._common import (
    chembl_id_column,
    create_chembl_schema,
    non_negative_int_column,
    standard_string_column,
)

SCHEMA_VERSION = "1.1.0"

COLUMN_ORDER = (
    "assay_chembl_id",
    "row_subtype",
    "row_index",

    "description",
    "assay_type",
    "assay_type_description",
    "assay_test_type",
    "assay_category",
    "assay_organism",
    "assay_tax_id",
    "assay_strain",
    "assay_tissue",
    "assay_cell_type",
    "assay_subcellular_fraction",
    "target_chembl_id",
    "document_chembl_id",
    "src_id",
    "src_assay_id",
    "cell_chembl_id",
    "tissue_chembl_id",
    "assay_group",
    "confidence_score",
    "confidence_description",
    "variant_sequence",
    "assay_classifications",
    "assay_parameters",
    "assay_class_id",
    "curation_level",
)

AssaySchema = create_chembl_schema(
    {
        "assay_chembl_id": chembl_id_column(nullable=False, unique=True),
        "row_subtype": standard_string_column(nullable=False),
        "row_index": non_negative_int_column(nullable=False),
        "description": standard_string_column(),
        "assay_type": standard_string_column(),
        "assay_type_description": standard_string_column(),
        "assay_test_type": standard_string_column(),
        "assay_category": standard_string_column(),
        "assay_organism": standard_string_column(),
        "assay_tax_id": standard_string_column(),  # String type, not Int64
        "assay_strain": standard_string_column(),
        "assay_tissue": standard_string_column(),
        "assay_cell_type": standard_string_column(),
        "assay_subcellular_fraction": standard_string_column(),
        "target_chembl_id": chembl_id_column(),
        "document_chembl_id": chembl_id_column(),
        "src_id": standard_string_column(),
        "src_assay_id": standard_string_column(),
        "cell_chembl_id": chembl_id_column(),
        "tissue_chembl_id": chembl_id_column(),
        "assay_group": standard_string_column(),
        "confidence_score": Column(pa.Int64, nullable=True),  # type: ignore[assignment]
        "confidence_description": standard_string_column(),
        "variant_sequence": standard_string_column(),
        "assay_classifications": standard_string_column(),
        "assay_parameters": standard_string_column(),
        "assay_class_id": standard_string_column(),
        "curation_level": standard_string_column(),
    },
    schema_name="AssaySchema",
    version=SCHEMA_VERSION,
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "AssaySchema"]

