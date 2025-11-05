"""Pandera schema describing the normalized ChEMBL activity dataset."""

from __future__ import annotations

import json
import math
from numbers import Number
from typing import cast

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema

SCHEMA_VERSION = "1.5.0"

COLUMN_ORDER = (
    "activity_id",
    "row_subtype",
    "row_index",
    "assay_chembl_id",
    "assay_type",
    "assay_description",
    "assay_organism",
    "assay_tax_id",
    "testitem_chembl_id",
    "molecule_chembl_id",
    "parent_molecule_chembl_id",
    "molecule_pref_name",
    "target_chembl_id",
    "target_pref_name",
    "document_chembl_id",
    "record_id",
    "src_id",
    "type",
    "relation",
    "value",
    "units",
    "standard_type",
    "standard_relation",
    "standard_value",
    "standard_upper_value",
    "standard_units",
    "standard_text_value",
    "standard_flag",
    "upper_value",
    "lower_value",
    "pchembl_value",
    "uo_units",
    "qudt_units",
    "text_value",
    "activity_comment",
    "bao_endpoint",
    "bao_format",
    "bao_label",
    "canonical_smiles",
    "ligand_efficiency",
    "target_organism",
    "target_tax_id",
    "data_validity_comment",
    "data_validity_description",
    "potential_duplicate",
    "activity_properties",
    "compound_key",
    "compound_name",
    "curated",
    "removed",
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
    if isinstance(value, (float, int)) and not isinstance(value, bool):
        try:
            if math.isnan(float(value)):  # type: ignore[arg-type]
                return True
        except (TypeError, ValueError):
            pass
    if not isinstance(value, str):
        return False

    try:
        payload = json.loads(value)
    except (TypeError, ValueError):
        return False

    if not isinstance(payload, list):
        return False

    # Явная типизация для mypy
    payload_list: list[dict[str, object]] = cast(list[dict[str, object]], payload)
    for item in payload_list:
        if not _is_valid_activity_property_item(item):
            return False

    return True

ActivitySchema = DataFrameSchema(
    {
        "activity_id": Column(pa.Int64, Check.ge(1), nullable=False, unique=True),  # type: ignore[assignment]
        "row_subtype": Column(pa.String, nullable=False),  # type: ignore[assignment]
        "row_index": Column(pa.Int64, Check.ge(0), nullable=False),  # type: ignore[assignment]
        "assay_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=False),  # type: ignore[assignment]
        "assay_type": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "assay_description": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "assay_organism": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "assay_tax_id": Column(  # type: ignore[assignment]
            pa.Int64,  # type: ignore[arg-type]
            Check.ge(1),  # type: ignore[arg-type]
            nullable=True,
        ),
        "testitem_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=False),  # type: ignore[assignment]
        "molecule_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=False),  # type: ignore[assignment]
        "parent_molecule_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=True),  # type: ignore[assignment]
        "molecule_pref_name": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "target_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=True),  # type: ignore[assignment]
        "target_pref_name": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "document_chembl_id": Column(pa.String, Check.str_matches(r"^CHEMBL\d+$"), nullable=True),  # type: ignore[assignment]
        "record_id": Column(  # type: ignore[assignment]
            pa.Int64,  # type: ignore[arg-type]
            Check.ge(1),  # type: ignore[arg-type]
            nullable=True,
        ),
        "src_id": Column(  # type: ignore[assignment]
            pa.Int64,  # type: ignore[arg-type]
            Check.ge(1),  # type: ignore[arg-type]
            nullable=True,
        ),
        "type": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "relation": Column(pa.String, Check.isin(RELATIONS), nullable=True),  # type: ignore[assignment]
        "value": Column(pa.Object, nullable=True),  # type: ignore[assignment]
        "units": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "standard_type": Column(pa.String, Check.isin(STANDARD_TYPES), nullable=True),  # type: ignore[assignment]
        "standard_relation": Column(pa.String, Check.isin(RELATIONS), nullable=True),  # type: ignore[assignment]
        "standard_value": Column(pa.Float64, Check.ge(0), nullable=True),  # type: ignore[assignment]
        "standard_upper_value": Column(pa.Float64, Check.ge(0), nullable=True),  # type: ignore[assignment]
        "standard_units": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "standard_text_value": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "standard_flag": Column(pa.Int64, Check.isin({0, 1}), nullable=True),  # type: ignore[assignment]
        "upper_value": Column(pa.Float64, Check.ge(0), nullable=True),  # type: ignore[assignment]
        "lower_value": Column(pa.Float64, Check.ge(0), nullable=True),  # type: ignore[assignment]
        "pchembl_value": Column(pa.Float64, Check.ge(0), nullable=True),  # type: ignore[assignment]
        "uo_units": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "qudt_units": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "text_value": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "activity_comment": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "bao_endpoint": Column(pa.String, Check.str_matches(r"^BAO_\d{7}$"), nullable=True),  # type: ignore[assignment]
        "bao_format": Column(pa.String, Check.str_matches(r"^BAO_\d{7}$"), nullable=True),  # type: ignore[assignment]
        "bao_label": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "canonical_smiles": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "ligand_efficiency": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "target_organism": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "target_tax_id": Column(  # type: ignore[assignment]
            pa.Int64,  # type: ignore[arg-type]
            Check.ge(1),  # type: ignore[arg-type]
            nullable=True,
        ),
        "data_validity_comment": Column(
            pa.String,  # type: ignore[arg-type]
            nullable=True,
            # Soft enum: валидация через whitelist в pipeline.validate(), не через Check
            # Неизвестные значения логируются как warning, но не блокируют валидацию
        ),  # type: ignore[assignment]
        "data_validity_description": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "potential_duplicate": Column(pd.BooleanDtype(), nullable=True),  # type: ignore[assignment]
        "activity_properties": Column(  # type: ignore[assignment]
            pa.String,  # type: ignore[arg-type]
            Check(_is_valid_activity_properties, element_wise=True),
            nullable=True,
        ),
        "compound_key": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "compound_name": Column(pa.String, nullable=True),  # type: ignore[assignment]
        "curated": Column(pd.BooleanDtype(), nullable=True),  # type: ignore[assignment]
        "removed": Column(pd.BooleanDtype(), nullable=True),  # type: ignore[assignment]
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
