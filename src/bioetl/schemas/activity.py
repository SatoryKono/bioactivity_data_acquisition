"""Pandera schema describing the normalized ChEMBL activity dataset."""

from __future__ import annotations

import json
import math
from numbers import Number

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema

SCHEMA_VERSION = "1.3.0"

COLUMN_ORDER = (
    "activity_id",
    "assay_chembl_id",
    "testitem_chembl_id",
    "molecule_chembl_id",
    "target_chembl_id",
    "document_chembl_id",
    "type",
    "relation",
    "value",
    "units",
    "standard_type",
    "standard_relation",
    "standard_value",
    "standard_units",
    "standard_text_value",
    "standard_flag",
    "upper_value",
    "lower_value",
    "pchembl_value",
    "activity_comment",
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
ACTIVITY_PROPERTY_KEYS = (
    "type",
    "relation",
    "units",
    "value",
    "text_value",
    "result_flag",
)


def _is_valid_activity_property_item(item: dict[str, object]) -> bool:
    """Return True if the payload item only contains the allowed keys and value types."""

    if set(item.keys()) != set(ACTIVITY_PROPERTY_KEYS):
        return False

    type_value = item["type"]
    if type_value is not None and not isinstance(type_value, str):
        return False

    relation_value = item["relation"]
    if relation_value is not None and not isinstance(relation_value, str):
        return False

    units_value = item["units"]
    if units_value is not None and not isinstance(units_value, str):
        return False

    value_value = item["value"]
    if value_value is not None and not isinstance(value_value, (Number, str)):
        return False

    text_value = item["text_value"]
    if text_value is not None and not isinstance(text_value, str):
        return False

    result_flag = item["result_flag"]
    if result_flag is not None and not isinstance(result_flag, bool):
        if isinstance(result_flag, int):
            if result_flag not in (0, 1):
                return False
        else:
            return False

    return True


def _is_valid_activity_properties(value: object) -> bool:
    """Element-wise validator ensuring activity_properties stores normalized JSON arrays."""

    if value is None:
        return True
    if value is pd.NA:
        return True
    if isinstance(value, Number) and not isinstance(value, bool):
        try:
            if math.isnan(value):
                return True
        except TypeError:
            pass
    if not isinstance(value, str):
        return False

    try:
        payload = json.loads(value)
    except (TypeError, ValueError):
        return False

    if not isinstance(payload, list):
        return False

    for item in payload:
        if not isinstance(item, dict):
            return False
        if not _is_valid_activity_property_item(item):
            return False

    return True

ActivitySchema = DataFrameSchema(
    {
        "activity_id": Column(pa.Int64, Check.ge(1), nullable=False, unique=True),
        "assay_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=False),
        "testitem_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=False),
        "molecule_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=False),
        "target_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=True),
        "document_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=True),
        "type": Column(pa.String, nullable=True),
        "relation": Column(pa.String, Check.isin(RELATIONS), nullable=True),
        "value": Column(pa.Object, nullable=True),
        "units": Column(pa.String, nullable=True),
        "standard_type": Column(pa.String, Check.isin(STANDARD_TYPES), nullable=True),
        "standard_relation": Column(pa.String, Check.isin(RELATIONS), nullable=True),
        "standard_value": Column(pa.Float64, Check.ge(0), nullable=True),
        "standard_units": Column(pa.String, nullable=True),
        "standard_text_value": Column(pa.String, nullable=True),
        "standard_flag": Column(pa.Int64, Check.isin({0, 1}), nullable=True),
        "upper_value": Column(pa.Float64, Check.ge(0), nullable=True),
        "lower_value": Column(pa.Float64, Check.ge(0), nullable=True),
        "pchembl_value": Column(pa.Float64, Check.ge(0), nullable=True),
        "activity_comment": Column(pa.String, nullable=True),
        "bao_endpoint": Column(pa.String, Check.str_matches(r"^BAO_\d{7}$"), nullable=True),
        "bao_format": Column(pa.String, Check.str_matches(r"^BAO_\d{7}$"), nullable=True),
        "bao_label": Column(pa.String, nullable=True),
        "canonical_smiles": Column(pa.String, nullable=True),
        "ligand_efficiency": Column(pa.String, nullable=True),
        "target_organism": Column(pa.String, nullable=True),
        "target_tax_id": Column(
            pa.Int64,
            Check.ge(1),
            nullable=True,
        ),
        "data_validity_comment": Column(pa.String, nullable=True),
        "potential_duplicate": Column(pa.Bool, nullable=True),
        "activity_properties": Column(
            pa.String,
            Check(_is_valid_activity_properties, element_wise=True),
            nullable=True,
        ),
        "compound_key": Column(pa.String, nullable=True),
    },
    ordered=True,
    coerce=False,  # Disable coercion at schema level - types are normalized in transform
    name=f"ActivitySchema_v{SCHEMA_VERSION}",
)

__all__ = [
    "SCHEMA_VERSION",
    "COLUMN_ORDER",
    "STANDARD_TYPES",
    "RELATIONS",
    "ACTIVITY_PROPERTY_KEYS",
    "ActivitySchema",
]
