"""Pandera schema describing the normalized ChEMBL target dataset."""

from __future__ import annotations

from bioetl.schemas.base import create_schema
from bioetl.schemas.common import (
    chembl_id_column,
    nullable_pd_int64_column,
    nullable_string_column,
    string_column_with_check,
)
from bioetl.schemas.vocab import required_vocab_ids
TARGET_TYPES = required_vocab_ids("target_type")


SCHEMA_VERSION = "1.1.0"

COLUMN_ORDER = (
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
)

TargetSchema = create_schema(
    columns={
        "target_chembl_id": chembl_id_column(nullable=False, unique=True),
        "pref_name": nullable_string_column(),
        "target_type": string_column_with_check(isin=TARGET_TYPES),
        "organism": nullable_string_column(),
        "tax_id": nullable_string_column(),
        "species_group_flag": nullable_pd_int64_column(isin={0, 1}),
        "cross_references__flat": nullable_string_column(),
        "target_components__flat": nullable_string_column(),
        "target_component_synonyms__flat": nullable_string_column(),
        "uniprot_accessions": nullable_string_column(),
        "protein_class_desc": nullable_string_column(),
        "protein_class_list": nullable_string_column(),
        "protein_class_top": nullable_string_column(),
        "component_count": nullable_pd_int64_column(ge=0),
        "hash_row": string_column_with_check(str_length=(64, 64), nullable=False),
        "hash_business_key": string_column_with_check(str_length=(64, 64), nullable=True),
    },
    version=SCHEMA_VERSION,
    name="TargetSchema",
    strict=True,
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "TARGET_TYPES", "TargetSchema"]

