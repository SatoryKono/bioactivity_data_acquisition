"""Tests for ChEMBL parsers and normalizers."""

from __future__ import annotations

import pandas as pd

from bioetl.pipelines.chembl.activity.normalizers import ActivityNormalizer
from bioetl.pipelines.chembl.activity.parsers import ActivityParser
from bioetl.clients.chembl.normalization import (
    BaseChemblNormalizer,
    ColumnMapping,
    ColumnNormalizationSpec,
    build_records_from_payload,
)
from bioetl.schemas.chembl_activity_schema import (
    ChEMBLActivityColumns,
    ChEMBLActivitySchema,
)


def test_activity_parser_extracts_columns():
    """Test that ActivityParser correctly extracts full ChEMBL schema
    columns."""
    # Mock payload with representative fields from the full ChEMBL
    # schema
    payload = {
        "results": [
            {
                "activity_id": "ACT1",
                "assay_chembl_id": "ASSAY1",
                "target_chembl_id": "TGT1",
                "molecule_chembl_id": "MOL1",
                "document_chembl_id": "DOC1",
                "standard_value": "5.0",
                "standard_units": "nM",
                "standard_type": "IC50",
                "pchembl_value": "7.3",
                "standard_relation": "=",
                "canonical_smiles": "CCO",
                "target_organism": "Homo sapiens",
                "activity_comment": "Test comment",
            }
        ]
    }

    df = ActivityParser().parse(payload)

    # Verify key columns from the full schema are present and correctly mapped
    expected_columns = [
        "activity_id",
        "assay_chembl_id",
        "target_chembl_id",
        "molecule_chembl_id",
        "document_chembl_id",
        "standard_value",
        "standard_units",
        "standard_type",
        "pchembl_value",
        "standard_relation",
        "canonical_smiles",
        "target_organism",
        "activity_comment",
    ]

    # Check that all expected columns exist in the DataFrame
    for col in expected_columns:
        assert col in df.columns, f"Missing column: {col}"

    # Verify the first row has correct values
    assert df.iloc[0]["activity_id"] == "ACT1"
    assert df.iloc[0]["assay_chembl_id"] == "ASSAY1"
    assert df.iloc[0]["target_chembl_id"] == "TGT1"
    assert df.iloc[0]["molecule_chembl_id"] == "MOL1"
    assert df.iloc[0]["standard_value"] == "5.0"
    assert df.iloc[0]["standard_units"] == "nM"
    assert df.iloc[0]["standard_type"] == "IC50"
    assert df.iloc[0]["pchembl_value"] == "7.3"

    # Verify DataFrame has all columns from the full ChEMBL schema
    assert len(df.columns) > 40, f"Expected 40+ columns, got {len(df.columns)}"


def test_activity_normalizer_coerces_types():
    """Test that ActivityNormalizer correctly coerces column types."""
    raw_df = pd.DataFrame(
        {
            "activity_id": ["1"],
            "assay_id": ["2"],
            "target_id": [None],
            "value": ["1.5"],
            "unit": ["uM"],
        }
    )

    normalized = ActivityNormalizer().normalize(raw_df)

    assert normalized["value"].dtype.kind == "f"
    assert normalized.loc[0, "value"] == 1.5
    assert normalized.loc[0, "business_key"] == "1"
    assert normalized.loc[0, "row_hash"]


def test_build_records_from_payload_respects_mappings():
    """Test that payload builder respects column mappings and fallbacks."""
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
    """Test that BaseChemblNormalizer applies defaults and type coercion."""
    df_raw = pd.DataFrame(
        {
            "activity_id": ["10"],
            "assay_id": ["20"],
            "value": ["2.5"],
        }
    )
    normalizer = BaseChemblNormalizer(
        business_key_column="activity_id",
        schema=ChEMBLActivitySchema,
        columns=ChEMBLActivityColumns,
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
