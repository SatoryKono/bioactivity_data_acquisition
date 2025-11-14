"""Pandera schema describing the normalized ChEMBL assay dataset."""

from __future__ import annotations

from bioetl.schemas import base_abstract_schema, common_column_factory
from bioetl.schemas.common_schema import resolve_row_hash_fields

SCHEMA_VERSION = "1.3.0"

COLUMN_ORDER: list[str] = [
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
    "hash_row",
    "hash_business_key",
    "load_meta_id",
]

REQUIRED_FIELDS: list[str] = [
    "assay_chembl_id",
    "row_subtype",
    "row_index",
    "load_meta_id",
    "hash_row",
]

BUSINESS_KEY_FIELDS: list[str] = [
    "assay_chembl_id",
    "row_subtype",
    "row_index",
]

ROW_HASH_FIELDS: list[str] = list(resolve_row_hash_fields(COLUMN_ORDER))

# Row metadata columns
CF = common_column_factory.SchemaColumnFactory
row_meta = CF.row_metadata()

AssaySchema = base_abstract_schema.create_schema(
    columns={
        "assay_chembl_id": CF.chembl_id(nullable=False, unique=True),
        **row_meta,
        "description": CF.string(),
        "assay_type": CF.string(vocabulary="assay_type"),
        "assay_type_description": CF.string(),
        "assay_test_type": CF.string(),
        "assay_category": CF.string(),
        "assay_organism": CF.string(),
        "assay_tax_id": CF.string(),
        "assay_strain": CF.string(),
        "assay_tissue": CF.string(),
        "assay_cell_type": CF.string(),
        "assay_subcellular_fraction": CF.string(),
        "target_chembl_id": CF.chembl_id(),
        "document_chembl_id": CF.chembl_id(),
        "src_id": CF.string(),
        "src_assay_id": CF.string(),
        "cell_chembl_id": CF.chembl_id(),
        "tissue_chembl_id": CF.chembl_id(),
        "assay_group": CF.string(),
        "confidence_score": CF.int64(),
        "confidence_description": CF.string(),
        "variant_sequence": CF.string(),
        "assay_classifications": CF.string(),
        "assay_parameters": CF.string(),
        "assay_class_id": CF.string(),
        "curation_level": CF.string(),
        "hash_row": CF.string(length=(64, 64), nullable=False),
        "hash_business_key": CF.string(length=(64, 64)),
        "load_meta_id": CF.uuid(nullable=False),
    },
    version=SCHEMA_VERSION,
    name="AssaySchema",
    column_order=COLUMN_ORDER,
)

__all__ = [
    "SCHEMA_VERSION",
    "COLUMN_ORDER",
    "REQUIRED_FIELDS",
    "BUSINESS_KEY_FIELDS",
    "ROW_HASH_FIELDS",
    "AssaySchema",
]
