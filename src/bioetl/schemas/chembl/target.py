"""Pandera schema describing the normalized ChEMBL target dataset."""

from __future__ import annotations

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema

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

TargetSchema = DataFrameSchema(
    {
        "target_chembl_id": Column(  # type: ignore[assignment]
            pa.String,  # type: ignore[arg-type]
            Check.str_matches(r"^CHEMBL\d+$"),  # type: ignore[arg-type]
            nullable=False,
            unique=True,
        ),
        "pref_name": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "target_type": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "organism": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "tax_id": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "species_group_flag": Column(pd.Int64Dtype(), Check.isin([0, 1]), nullable=True),  # type: ignore[arg-type]
        "cross_references__flat": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "target_components__flat": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "target_component_synonyms__flat": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "uniprot_accessions": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "protein_class_desc": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "component_count": Column(pd.Int64Dtype(), Check.ge(0), nullable=True),  # type: ignore[arg-type]
    },
    strict=True,
    ordered=True,
    coerce=False,  # Disable coercion at schema level - types are normalized in transform
    name=f"TargetSchema_v{SCHEMA_VERSION}",
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "TargetSchema"]

