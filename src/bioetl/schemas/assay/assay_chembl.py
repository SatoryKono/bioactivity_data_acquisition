"""Pandera schema describing the normalized ChEMBL assay dataset."""

from __future__ import annotations

from bioetl.schemas.base import create_schema
from bioetl.schemas.common import (
    chembl_id_column,
    nullable_int64_column,
    nullable_string_column,
    row_metadata_columns,
    string_column_with_check,
)
from bioetl.schemas.vocab import required_vocab_ids
ASSAY_TYPES = required_vocab_ids("assay_type")


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

# Row metadata columns
row_meta = row_metadata_columns()

AssaySchema = create_schema(
    columns={
        "assay_chembl_id": chembl_id_column(nullable=False, unique=True),
        **row_meta,
        "description": nullable_string_column(),
        "assay_type": string_column_with_check(isin=ASSAY_TYPES),
        "assay_type_description": nullable_string_column(),
        "assay_test_type": nullable_string_column(),
        "assay_category": nullable_string_column(),
        "assay_organism": nullable_string_column(),
        "assay_tax_id": nullable_string_column(),
        "assay_strain": nullable_string_column(),
        "assay_tissue": nullable_string_column(),
        "assay_cell_type": nullable_string_column(),
        "assay_subcellular_fraction": nullable_string_column(),
        "target_chembl_id": chembl_id_column(),
        "document_chembl_id": chembl_id_column(),
        "src_id": nullable_string_column(),
        "src_assay_id": nullable_string_column(),
        "cell_chembl_id": chembl_id_column(),
        "tissue_chembl_id": chembl_id_column(),
        "assay_group": nullable_string_column(),
        "confidence_score": nullable_int64_column(),
        "confidence_description": nullable_string_column(),
        "variant_sequence": nullable_string_column(),
        "assay_classifications": nullable_string_column(),
        "assay_parameters": nullable_string_column(),
        "assay_class_id": nullable_string_column(),
        "curation_level": nullable_string_column(),
    },
    version=SCHEMA_VERSION,
    name="AssaySchema",
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "ASSAY_TYPES", "AssaySchema"]

