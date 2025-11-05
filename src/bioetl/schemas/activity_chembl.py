"""Pandera schema describing the normalized ChEMBL activity dataset."""

from __future__ import annotations

import json
import math
from numbers import Number
from typing import cast

import pandas as pd
import pandera as pa
from pandera import Check, Column

from ._common import (
    bao_id_column,
    chembl_id_column,
    create_chembl_schema,
    non_negative_float_column,
    non_negative_int_column,
    positive_int_column,
    standard_string_column,
    tax_id_column,
)

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

STANDARD_TYPES = {"IC50",  "Ki"} #"EC50", "XC50", "AC50", "Kd", "Potency", "ED50"
RELATIONS = {"="} #, ">", "<", ">=", "<=", "~"

# Допустимые единицы измерения для колонки units (все варианты)
ALLOWED_UNITS = {
    # Канонические формы
    "nM",
    "μM",
    "mM",
    "pM",
    # Варианты для nM
    "nanomolar",
    "nmol",
    "nm",
    "NM",
    # Варианты для μM
    "µM",
    "uM",
    "UM",
    "micromolar",
    "microM",
    "umol",
    # Варианты для mM
    "millimolar",
    "milliM",
    "mmol",
    "MM",
    # Варианты для pM
    "picomolar",
    "picomol",
    "PM",
}

# Допустимые единицы измерения для колонки standard_units (только молярные концентрации)
ALLOWED_STANDARD_UNITS = {
    # Канонические формы
    "nM",
    "μM",
    "mM",
    "pM",
    # Варианты для nM
    "nanomolar",
    "nmol",
    "nm",
    "NM",
    # Варианты для μM
    "µM",
    "uM",
    "UM",
    "micromolar",
    "microM",
    "umol",
    # Варианты для mM
    "millimolar",
    "milliM",
    "mmol",
    "MM",
    # Варианты для pM
    "picomolar",
    "picomol",
    "PM",
}

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

ActivitySchema = create_chembl_schema(
    {
        "activity_id": Column(pa.Int64, Check.ge(1), nullable=False, unique=True),  # type: ignore[assignment]
        "row_subtype": standard_string_column(nullable=False),
        "row_index": non_negative_int_column(nullable=False),
        "assay_chembl_id": chembl_id_column(nullable=False),
        "assay_type": standard_string_column(),
        "assay_description": standard_string_column(),
        "assay_organism": standard_string_column(),
        "assay_tax_id": tax_id_column(),
        "testitem_chembl_id": chembl_id_column(nullable=False),
        "molecule_chembl_id": chembl_id_column(nullable=False),
        "parent_molecule_chembl_id": chembl_id_column(),
        "molecule_pref_name": standard_string_column(),
        "target_chembl_id": chembl_id_column(),
        "target_pref_name": standard_string_column(),
        "document_chembl_id": chembl_id_column(),
        "record_id": positive_int_column(),
        "src_id": positive_int_column(),
        "type": standard_string_column(),
        "relation": Column(pa.String, Check.isin(RELATIONS), nullable=True),  # type: ignore[assignment]
        "value": Column(pa.Object, nullable=True),  # type: ignore[assignment]
        "units": Column(pa.String, Check.isin(ALLOWED_UNITS), nullable=True),  # type: ignore[assignment]
        "standard_type": Column(pa.String, Check.isin(STANDARD_TYPES), nullable=True),  # type: ignore[assignment]
        "standard_relation": Column(pa.String, Check.isin(RELATIONS), nullable=True),  # type: ignore[assignment]
        "standard_value": non_negative_float_column(),
        "standard_upper_value": non_negative_float_column(),
        "standard_units": Column(pa.String, Check.isin(ALLOWED_STANDARD_UNITS), nullable=True),  # type: ignore[assignment]
        "standard_text_value": standard_string_column(),
        "standard_flag": Column(pa.Int64, Check.isin({0, 1}), nullable=True),  # type: ignore[assignment]
        "upper_value": non_negative_float_column(),
        "lower_value": non_negative_float_column(),
        "pchembl_value": non_negative_float_column(),
        "uo_units": standard_string_column(),
        "qudt_units": standard_string_column(),
        "text_value": standard_string_column(),
        "activity_comment": standard_string_column(),
        "bao_endpoint": bao_id_column(),
        "bao_format": bao_id_column(),
        "bao_label": standard_string_column(),
        "canonical_smiles": standard_string_column(),
        "ligand_efficiency": standard_string_column(),
        "target_organism": standard_string_column(),
        "target_tax_id": tax_id_column(),
        "data_validity_comment": standard_string_column(),
        # Soft enum: валидация через whitelist в pipeline.validate(), не через Check
        # Неизвестные значения логируются как warning, но не блокируют валидацию
        "data_validity_description": standard_string_column(),
        "potential_duplicate": Column(pd.BooleanDtype(), nullable=True),  # type: ignore[assignment]
        "activity_properties": Column(  # type: ignore[assignment]
            pa.String,  # type: ignore[arg-type]
            Check(_is_valid_activity_properties, element_wise=True),
            nullable=True,
        ),
        "compound_key": standard_string_column(),
        "compound_name": standard_string_column(),
        "curated": Column(pd.BooleanDtype(), nullable=True),  # type: ignore[assignment]
        "removed": Column(pd.BooleanDtype(), nullable=True),  # type: ignore[assignment]
    },
    schema_name="ActivitySchema",
    version=SCHEMA_VERSION,
)

__all__ = [
    "SCHEMA_VERSION",
    "COLUMN_ORDER",
    "STANDARD_TYPES",
    "RELATIONS",
    "ALLOWED_UNITS",
    "ALLOWED_STANDARD_UNITS",
    "ACTIVITY_PROPERTY_KEYS",
    "ActivitySchema",
]

