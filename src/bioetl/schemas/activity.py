"""Pandera schema describing the normalized ChEMBL activity dataset."""

from __future__ import annotations

import pandera.pandas as pa
from pandera import Check, Column, DataFrameSchema

SCHEMA_VERSION = "1.2.0"

COLUMN_ORDER = (
    "activity_id",
    "assay_chembl_id",
    "testitem_chembl_id",
    "molecule_chembl_id",
    "target_chembl_id",
    "document_chembl_id",
    "standard_type",
    "standard_relation",
    "standard_value",
    "standard_units",
    "pchembl_value",
    "bao_endpoint",
    "bao_format",
    "bao_label",
    "canonical_smiles",
    "ligand_efficiency",
    "target_organism",
    "target_tax_id",
    "data_validity_comment",
    "potential_duplicate",
    "activity_properties",
    "compound_key",
)

STANDARD_TYPES = {"IC50", "EC50", "XC50", "AC50", "Ki", "Kd", "Potency", "ED50"}
RELATIONS = {"=", ">", "<", ">=", "<=", "~"}

ActivitySchema = DataFrameSchema(
    {
        "activity_id": Column(pa.Int64, Check.ge(1), nullable=False, unique=True),  # type: ignore[unknown]
        "assay_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=False),  # type: ignore[unknown]
        "testitem_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=False),  # type: ignore[unknown]
        "molecule_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=False),  # type: ignore[unknown]
        "target_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=True),  # type: ignore[unknown]
        "document_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=True),  # type: ignore[unknown]
        "standard_type": Column(pa.String, Check.isin(STANDARD_TYPES), nullable=True),  # type: ignore[unknown]
        "standard_relation": Column(pa.String, Check.isin(RELATIONS), nullable=True),  # type: ignore[unknown]
        "standard_value": Column(pa.Float64, Check.ge(0), nullable=True),  # type: ignore[unknown]
        "standard_units": Column(pa.String, nullable=True),  # type: ignore[unknown]
        "pchembl_value": Column(pa.Float64, Check.ge(0), nullable=True),  # type: ignore[unknown]
        "bao_endpoint": Column(pa.String, Check.str_matches(r"^BAO_\d{7}$"), nullable=True),  # type: ignore[unknown]
        "bao_format": Column(pa.String, Check.str_matches(r"^BAO_\d{7}$"), nullable=True),  # type: ignore[unknown]
        "bao_label": Column(pa.String, nullable=True),  # type: ignore[unknown]
        "canonical_smiles": Column(pa.String, nullable=True),  # type: ignore[unknown]
        "ligand_efficiency": Column(pa.String, nullable=True),  # type: ignore[unknown]
        "target_organism": Column(pa.String, nullable=True),  # type: ignore[unknown]
        "target_tax_id": Column(  # type: ignore[unknown]
            pa.Int64,  # type: ignore[unknown]
            Check.ge(1),  # type: ignore[unknown]
            nullable=True,
        ),
        "data_validity_comment": Column(pa.String, nullable=True),  # type: ignore[unknown]
        "potential_duplicate": Column(pa.Bool, nullable=True),  # type: ignore[unknown]
        "activity_properties": Column(pa.String, nullable=True),  # type: ignore[unknown]
        "compound_key": Column(pa.String, nullable=True),  # type: ignore[unknown]
    },
    ordered=True,
    coerce=False,  # Disable coercion at schema level - types are normalized in transform
    name=f"ActivitySchema_v{SCHEMA_VERSION}",
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "STANDARD_TYPES", "RELATIONS", "ActivitySchema"]
