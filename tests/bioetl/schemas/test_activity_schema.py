"""Contract tests for the canonical ActivitySchema."""

from __future__ import annotations

import json

import pandas as pd
import pandera.errors
import pytest

from bioetl.schemas.activity import (
    ACTIVITY_PROPERTY_KEYS,
    COLUMN_ORDER,
    RELATIONS,
    ActivitySchema,
)


def _activity_properties_payload(**overrides: object) -> str:
    base = {
        "type": "IC50",
        "relation": "=",
        "units": "nM",
        "value": 10.0,
        "text_value": "10",
        "result_flag": True,
    }
    base.update(overrides)
    return json.dumps([base])


def _activity_record(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "activity_id": 1,
        "row_subtype": "activity",
        "row_index": 0,
        "assay_chembl_id": "CHEMBL1",
        "assay_type": "B",
        "assay_description": "desc",
        "assay_organism": "Homo sapiens",
        "assay_tax_id": 9606,
        "testitem_chembl_id": "CHEMBL2",
        "molecule_chembl_id": "CHEMBL3",
        "parent_molecule_chembl_id": "CHEMBL4",
        "molecule_pref_name": "Mol",
        "target_chembl_id": "CHEMBL5",
        "target_pref_name": "Target",
        "document_chembl_id": "CHEMBL6",
        "record_id": 11,
        "src_id": 22,
        "type": "IC50",
        "relation": next(iter(RELATIONS)),
        "value": 10.0,
        "units": "nM",
        "standard_type": "pIC50",
        "standard_relation": next(iter(RELATIONS)),
        "standard_value": 10.0,
        "standard_upper_value": 10.5,
        "standard_units": "nM",
        "standard_text_value": "10 nM",
        "standard_flag": 1,
        "upper_value": 12.0,
        "lower_value": 8.0,
        "pchembl_value": 7.0,
        "uo_units": "UO:0000065",
        "qudt_units": "Nanomolar",
        "text_value": "10",
        "activity_comment": "comment",
        "bao_endpoint": "BAO_0000001",
        "bao_format": "BAO_0000002",
        "bao_label": "label",
        "canonical_smiles": "C",
        "ligand_efficiency": "0.5",
        "target_organism": "Homo sapiens",
        "target_tax_id": 9606,
        "data_validity_comment": "ok",
        "data_validity_description": "ok",
        "potential_duplicate": False,
        "activity_properties": _activity_properties_payload(),
        "compound_key": "cmpd-1",
        "compound_name": "Compound",
        "curated": True,
        "removed": False,
        "load_meta_id": "123e4567-e89b-12d3-a456-426614174000",
        "hash_row": "0" * 64,
        "hash_business_key": "f" * 64,
    }
    payload.update(overrides)
    return payload


def _frame(payload: dict[str, object] | None = None) -> pd.DataFrame:
    record = _activity_record()
    if payload:
        record.update(payload)
    df = pd.DataFrame([record], columns=COLUMN_ORDER)
    df = df.astype(
        {
            "potential_duplicate": "boolean",
            "curated": "boolean",
            "removed": "boolean",
        }
    )
    df["value"] = df["value"].astype("object")
    return df


def test_activity_schema_accepts_valid_record() -> None:
    df = _frame()
    ActivitySchema.validate(df, lazy=True)


def test_activity_schema_rejects_invalid_relation() -> None:
    invalid_relation = {"relation": "!"}
    df = _frame(invalid_relation)
    with pytest.raises(pandera.errors.SchemaErrors):
        ActivitySchema.validate(df, lazy=True)


def test_activity_schema_enforces_column_order_metadata() -> None:
    assert tuple(COLUMN_ORDER) == ActivitySchema.metadata.get("column_order")


def test_activity_properties_validator_rejects_partial_items() -> None:
    invalid_properties = json.dumps(
        [
            {key: None for key in ACTIVITY_PROPERTY_KEYS if key != "type"},
        ]
    )
    df = _frame({"activity_properties": invalid_properties})
    with pytest.raises(pandera.errors.SchemaErrors):
        ActivitySchema.validate(df, lazy=True)

