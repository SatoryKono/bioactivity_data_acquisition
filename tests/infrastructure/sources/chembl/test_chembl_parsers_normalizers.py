from __future__ import annotations

import pandas as pd

from bioetl.infrastructure.sources.chembl.activity.normalizer import normalize_activity
from bioetl.infrastructure.sources.chembl.activity.parser import parse_activity_payload
from bioetl.infrastructure.sources.chembl.common import (
    BaseChemblNormalizer,
    ColumnMapping,
    ColumnNormalizationSpec,
    build_records_from_payload,
)
from bioetl.core.schemas.activity_schema import ActivityColumns, ActivitySchema


def test_activity_parser_extracts_columns():
    payload = {
        "results": [
            {
                "activity_id": "ACT1",
                "assay_chembl_id": "ASSAY1",
                "target_chembl_id": "TGT1",
                "standard_value": "5.0",
                "standard_units": "nM",
            }
        ]
    }

    df = parse_activity_payload(payload)

    assert list(df.columns) == [
        "activity_id",
        "assay_id",
        "target_id",
        "value",
        "unit",
    ]
    assert df.iloc[0].to_dict() == {
        "activity_id": "ACT1",
        "assay_id": "ASSAY1",
        "target_id": "TGT1",
        "value": "5.0",
        "unit": "nM",
    }


def test_activity_normalizer_coerces_types():
    raw_df = pd.DataFrame(
        {
            "activity_id": ["1"],
            "assay_id": ["2"],
            "target_id": [None],
            "value": ["1.5"],
            "unit": ["uM"],
        }
    )

    normalized = normalize_activity(raw_df)

    assert normalized["value"].dtype.kind == "f"
    assert normalized.loc[0, "value"] == 1.5
    assert normalized.loc[0, "business_key"] == "1"
    assert normalized.loc[0, "row_hash"]


def test_build_records_from_payload_respects_mappings():
    payload = {
        "results": [
            {"primary_id": "A1", "fallback_id": "B1", "name": "first"},
            {"fallback_id": "B2", "name": "second"},
        ]
    }

    mappings = [
        ColumnMapping("target", ("primary_id", "fallback_id")),
        ColumnMapping("label", ("name",)),
    ]

    records = build_records_from_payload(payload, mappings)

    assert records == [
        {"target": "A1", "label": "first"},
        {"target": "B2", "label": "second"},
    ]


def test_base_normalizer_applies_defaults_and_types():
    df_raw = pd.DataFrame(
        {
            "activity_id": ["10"],
            "assay_id": ["20"],
            "value": ["2.5"],
        }
    )
    normalizer = BaseChemblNormalizer(
        business_key_column="activity_id",
        schema=ActivitySchema,
        columns=ActivityColumns,
        column_specs=[
            ColumnNormalizationSpec("activity_id", dtype="string"),
            ColumnNormalizationSpec("assay_id", dtype="string"),
            ColumnNormalizationSpec(
                "target_id",
                dtype="string",
                default=pd.NA,
            ),
            ColumnNormalizationSpec(
                "value",
                dtype="float",
                transformer=lambda s: pd.to_numeric(
                    s,
                    errors="coerce",
                ),
            ),
            ColumnNormalizationSpec("unit", dtype="string", default=pd.NA),
        ],
    )

    normalized = normalizer.normalize(df_raw)

    assert (
        normalized.loc[0, "target_id"] is pd.NA
        or pd.isna(normalized.loc[0, "target_id"])
    )
    assert (
        normalized.loc[0, "unit"] is pd.NA
        or pd.isna(normalized.loc[0, "unit"])
    )
