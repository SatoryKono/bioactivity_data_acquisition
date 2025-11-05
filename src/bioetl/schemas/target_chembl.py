"""Pandera schema describing the normalized ChEMBL target dataset."""

from __future__ import annotations

import pandas as pd
import pandera as pa
from pandera import Column

from ._common import (
    boolean_flag_column,
    chembl_id_column,
    create_chembl_schema,
    non_negative_int_column,
    standard_string_column,
)

SCHEMA_VERSION = "1.0.0"

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
    "component_count",
)

TargetSchema = create_chembl_schema(
    {
        "target_chembl_id": chembl_id_column(nullable=False, unique=True),
        "pref_name": standard_string_column(),
        "target_type": standard_string_column(),
        "organism": standard_string_column(),
        "tax_id": standard_string_column(),  # String type, not Int64
        "species_group_flag": boolean_flag_column(),
        "cross_references__flat": standard_string_column(),
        "target_components__flat": standard_string_column(),
        "target_component_synonyms__flat": standard_string_column(),
        "uniprot_accessions": standard_string_column(),
        "protein_class_desc": standard_string_column(),
        "component_count": non_negative_int_column(dtype=pd.Int64Dtype()),
    },
    schema_name="TargetSchema",
    version=SCHEMA_VERSION,
    strict=True,
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "TargetSchema"]

