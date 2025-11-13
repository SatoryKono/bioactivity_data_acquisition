"""Pandera schema describing the normalized ChEMBL activity dataset."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from numbers import Number
from typing import Any, cast

import pandas as pd
import pandera as pa
from pandera import Check, Column

from bioetl.schemas.base_abstract_schema import create_schema
from bioetl.schemas.common_column_factory import SchemaColumnFactory

SCHEMA_VERSION = "1.7.0"

COLUMN_ORDER: list[str] = [
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
    "load_meta_id",
    "hash_row",
    "hash_business_key",
]

REQUIRED_FIELDS: list[str] = [
    "activity_id",
    "row_subtype",
    "row_index",
    "assay_chembl_id",
    "testitem_chembl_id",
    "molecule_chembl_id",
    "load_meta_id",
    "hash_row",
]

BUSINESS_KEY_FIELDS: list[str] = [
    "activity_id",
    "row_subtype",
    "row_index",
]

ROW_HASH_FIELDS: list[str] = [
    column for column in COLUMN_ORDER if column not in {"hash_row", "hash_business_key"}
]

RELATIONS = {"=", "<", ">", "~"}
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

    candidate_items: list[Mapping[str, Any]]
    if isinstance(payload, list):
        candidate_items = []
        for item_raw in payload:  # pyright: ignore[reportUnknownVariableType]
            item = cast(object, item_raw)
            if isinstance(item, Mapping):
                candidate_items.append(item)
            else:
                return False
    elif isinstance(payload, Mapping):
        candidate_items = []
        for item_raw in payload.values():
            if isinstance(item_raw, Mapping):
                candidate_items.append(item_raw)
            else:
                return False
    else:
        return False

    for item in candidate_items:
        if not _is_valid_activity_property_item(dict(item)):
            return False

    return True


# Row metadata columns
CF = SchemaColumnFactory
row_meta = CF.row_metadata()

ActivitySchema = create_schema(
    columns={
        "activity_id": CF.int64(nullable=False, ge=1, unique=True),
        **row_meta,
        "assay_chembl_id": CF.chembl_id(nullable=False),
        "assay_type": CF.string(),
        "assay_description": CF.string(),
        "assay_organism": CF.string(),
        "assay_tax_id": CF.int64(ge=1),
        "testitem_chembl_id": CF.chembl_id(nullable=False),
        "molecule_chembl_id": CF.chembl_id(nullable=False),
        "parent_molecule_chembl_id": CF.chembl_id(),
        "molecule_pref_name": CF.string(),
        "target_chembl_id": CF.chembl_id(),
        "target_pref_name": CF.string(),
        "document_chembl_id": CF.chembl_id(),
        "record_id": CF.int64(ge=1),
        "src_id": CF.int64(ge=1),
        "type": CF.string(),
        "relation": CF.string(isin=RELATIONS),
        "value": CF.object(),
        "units": CF.string(),
        "standard_type": CF.string(
            vocabulary="activity_standard_type",
            vocabulary_allowed_statuses=("active",),
        ),
        "standard_relation": CF.string(isin=RELATIONS),
        "standard_value": CF.float64(ge=0),
        "standard_upper_value": CF.float64(ge=0),
        "standard_units": CF.string(),
        "standard_text_value": CF.string(),
        "standard_flag": CF.int64(isin={0, 1}),
        "upper_value": CF.float64(ge=0),
        "lower_value": CF.float64(ge=0),
        "pchembl_value": CF.float64(ge=0),
        "uo_units": CF.string(),
        "qudt_units": CF.string(),
        "text_value": CF.string(),
        "activity_comment": CF.string(),
        "bao_endpoint": CF.bao_id(),
        "bao_format": CF.bao_id(),
        "bao_label": CF.string(),
        "canonical_smiles": CF.string(),
        "ligand_efficiency": CF.string(),
        "target_organism": CF.string(),
        "target_tax_id": CF.int64(ge=1),
        "data_validity_comment": CF.string(),
        # Soft enum: validated via whitelist in pipeline.validate(), not via Check.
        # Unknown values are logged as warnings but do not block validation.
        "data_validity_description": CF.string(),
        "potential_duplicate": CF.boolean_flag(),
        "activity_properties": Column(  # type: ignore[assignment]
            pa.String,  # type: ignore[arg-type]
            Check(_is_valid_activity_properties, element_wise=True),
            nullable=True,
        ),
        "compound_key": CF.string(),
        "compound_name": CF.string(),
        "curated": CF.boolean_flag(),
        "removed": CF.boolean_flag(),
        "load_meta_id": CF.uuid(nullable=False),
        "hash_row": CF.string(length=(64, 64), nullable=False),
        "hash_business_key": CF.string(length=(64, 64)),
    },
    version=SCHEMA_VERSION,
    name="ActivitySchema",
    column_order=COLUMN_ORDER,
)

__all__ = [
    "SCHEMA_VERSION",
    "COLUMN_ORDER",
    "REQUIRED_FIELDS",
    "BUSINESS_KEY_FIELDS",
    "ROW_HASH_FIELDS",
    "RELATIONS",
    "ACTIVITY_PROPERTY_KEYS",
    "ActivitySchema",
]
