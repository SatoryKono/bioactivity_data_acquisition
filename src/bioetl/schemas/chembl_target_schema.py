"""Pandera schema describing the normalized ChEMBL target dataset."""

from __future__ import annotations

from bioetl.schemas.base_abstract_schema import create_schema
from bioetl.schemas.common_column_factory import SchemaColumnFactory


SCHEMA_VERSION = "1.2.0"

COLUMN_ORDER: list[str] = [
    "target_chembl_id",
    "pref_name",
    "target_type",
    "organism",
    "tax_id",
    "species_group_flag",
    "cross_references__flat",
    "target_components__flat",
    "target_component_synonyms__flat",
    "uniprot_accessions",
    "protein_class_desc",
    "protein_class_list",
    "protein_class_top",
    "component_count",
    "hash_row",
    "hash_business_key",
    "load_meta_id",
]

REQUIRED_FIELDS: list[str] = [
    "target_chembl_id",
    "load_meta_id",
    "hash_row",
]

BUSINESS_KEY_FIELDS: list[str] = [
    "target_chembl_id",
]

ROW_HASH_FIELDS: list[str] = [
    column for column in COLUMN_ORDER if column not in {"hash_row", "hash_business_key"}
]

CF = SchemaColumnFactory

TargetSchema = create_schema(
    columns={
        "target_chembl_id": CF.chembl_id(nullable=False, unique=True),
        "pref_name": CF.string(),
        "target_type": CF.string(vocabulary="target_type"),
        "organism": CF.string(),
        "tax_id": CF.string(),
        "species_group_flag": CF.int64(pandas_nullable=True, isin={0, 1}),
        "cross_references__flat": CF.string(),
        "target_components__flat": CF.string(),
        "target_component_synonyms__flat": CF.string(),
        "uniprot_accessions": CF.string(),
        "protein_class_desc": CF.string(),
        "protein_class_list": CF.string(),
        "protein_class_top": CF.string(),
        "component_count": CF.int64(pandas_nullable=True, ge=0),
        "hash_row": CF.string(length=(64, 64), nullable=False),
        "hash_business_key": CF.string(length=(64, 64)),
        "load_meta_id": CF.uuid(nullable=False),
    },
    version=SCHEMA_VERSION,
    name="TargetSchema",
    strict=True,
    column_order=COLUMN_ORDER,
)

__all__ = [
    "SCHEMA_VERSION",
    "COLUMN_ORDER",
    "REQUIRED_FIELDS",
    "BUSINESS_KEY_FIELDS",
    "ROW_HASH_FIELDS",
    "TargetSchema",
]
