"""Pandera schema describing the normalized ChEMBL assay dataset."""

from __future__ import annotations

import pandera as pa
from pandera import Check, Column, DataFrameSchema

SCHEMA_VERSION = "1.0.0"

COLUMN_ORDER = (
    "assay_chembl_id",
    "row_subtype",
    "row_index",
    "assay_type",
    "assay_category",
    "assay_class_id",
    "assay_organism",
    "target_chembl_id",
    "confidence_score",
    "curation_level",
)

AssaySchema = DataFrameSchema(
    {
        "assay_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=False, unique=True),
        "row_subtype": Column(pa.String, nullable=False),
        "row_index": Column(pa.Int64, Check.ge(0), nullable=False),
        "assay_type": Column(pa.String, nullable=True),
        "assay_category": Column(pa.String, nullable=True),
        "assay_class_id": Column(pa.String, nullable=True),
        "assay_organism": Column(pa.String, nullable=True),
        "target_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=True),
        "confidence_score": Column(pa.Int64, nullable=True),
        "curation_level": Column(pa.String, nullable=True),
    },
    ordered=True,
    coerce=False,  # Disable coercion at schema level - types are normalized in transform
    name=f"AssaySchema_v{SCHEMA_VERSION}",
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "AssaySchema"]

