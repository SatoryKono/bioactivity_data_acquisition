"""Pandera schema describing the normalized ChEMBL activity dataset."""

from __future__ import annotations

import pandera as pa
from pandera import Check, Column, DataFrameSchema

SCHEMA_VERSION = "1.0.0"

COLUMN_ORDER = (
    "activity_id",
    "molecule_chembl_id",
    "assay_chembl_id",
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
    "activity_properties",
    "compound_key",
    "is_citation",
    "high_citation_rate",
    "exact_data_citation",
    "rounded_data_citation",
)

STANDARD_TYPES = {"IC50", "EC50", "XC50", "AC50", "Ki", "Kd", "Potency", "ED50"}
RELATIONS = {"=", ">", "<", ">=", "<=", "~"}

ActivitySchema = DataFrameSchema(
    {
        "activity_id": Column(pa.Int64, Check.ge(1), nullable=False, unique=True),
        "molecule_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=True),
        "assay_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=True),
        "target_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=True),
        "document_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=True),
        "standard_type": Column(pa.String, Check.isin(STANDARD_TYPES), nullable=True),
        "standard_relation": Column(pa.String, Check.isin(RELATIONS), nullable=True),
        "standard_value": Column(pa.Float64, Check.ge(0), nullable=True),
        "standard_units": Column(pa.String, nullable=True),
        "pchembl_value": Column(pa.Float64, Check.ge(0), nullable=True),
        "bao_endpoint": Column(pa.String, Check.str_matches(r"^BAO_\d{7}$"), nullable=True),
        "bao_format": Column(pa.String, Check.str_matches(r"^BAO_\d{7}$"), nullable=True),
        "bao_label": Column(pa.String, nullable=True),
        "canonical_smiles": Column(pa.String, nullable=True),
        "ligand_efficiency": Column(pa.String, nullable=True),
        "target_organism": Column(pa.String, nullable=True),
        "target_tax_id": Column(pa.Int64, Check.ge(1), nullable=True),
        "data_validity_comment": Column(pa.String, nullable=True),
        "activity_properties": Column(pa.String, nullable=True),
        "compound_key": Column(pa.String, nullable=True),
        "is_citation": Column(pa.Bool, nullable=True),
        "high_citation_rate": Column(pa.Bool, nullable=True),
        "exact_data_citation": Column(pa.Bool, nullable=True),
        "rounded_data_citation": Column(pa.Bool, nullable=True),
    },
    ordered=True,
    coerce=True,
    name=f"ActivitySchema_v{SCHEMA_VERSION}",
)

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "STANDARD_TYPES", "RELATIONS", "ActivitySchema"]
