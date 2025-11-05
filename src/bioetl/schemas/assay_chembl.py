"""Pandera schema describing the normalized ChEMBL assay dataset."""

from __future__ import annotations

import pandera as pa
from pandera import Check, Column, DataFrameSchema

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

AssaySchema = DataFrameSchema(
    {
        "assay_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=False, unique=True),  # type: ignore[assignment]
        "row_subtype": Column(pa.String, nullable=False),  # type: ignore[assignment]
        "row_index": Column(pa.Int64, Check.ge(0), nullable=False),  # type: ignore[assignment]
        "description": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "assay_type": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "assay_type_description": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "assay_test_type": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "assay_category": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "assay_organism": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "assay_tax_id": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "assay_strain": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "assay_tissue": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "assay_cell_type": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "assay_subcellular_fraction": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "target_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=True),  # type: ignore[assignment]
        "document_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=True),  # type: ignore[assignment]
        "src_id": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "src_assay_id": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "cell_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=True),  # type: ignore[assignment]
        "tissue_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=True),  # type: ignore[assignment]
        "assay_group": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "confidence_score": Column(pa.Int64, nullable=True),  # type: ignore[assignment]
        "confidence_description": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "variant_sequence": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "assay_classifications": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "assay_parameters": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "assay_class_id": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "curation_level": Column(pa.String, nullable=True),  # type: ignore[assignment]
    },
    ordered=True,
    coerce=False,  # Disable coercion at schema level - types are normalized in transform
    name=f"AssaySchema_v{SCHEMA_VERSION}",
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "AssaySchema"]

