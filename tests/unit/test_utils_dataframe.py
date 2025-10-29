"""Tests for dataframe utility helpers."""

import pandas as pd

from bioetl.core.hashing import generate_hash_business_key
from bioetl.schemas import ActivitySchema, TargetSchema
from bioetl.utils import finalize_pipeline_output
from bioetl.utils.dataframe import align_dataframe_to_schema


def test_finalize_pipeline_output_applies_metadata_and_order():
    """Ensure finalize_pipeline_output enforces determinism and metadata."""

    df = pd.DataFrame(
        [
            {"target_chembl_id": "CHEMBL2", "pref_name": "BETA", "custom_col": "x"},
            {"target_chembl_id": "CHEMBL1", "pref_name": "ALPHA"},
        ]
    )

    result = finalize_pipeline_output(
        df,
        business_key="target_chembl_id",
        sort_by=["target_chembl_id"],
        ascending=[True],
        pipeline_version="2.0.0",
        source_system="chembl",
        chembl_release="ChEMBL_TEST",
        extracted_at="2024-01-01T00:00:00+00:00",
        schema=TargetSchema,
    )

    assert list(result["target_chembl_id"]) == ["CHEMBL1", "CHEMBL2"]
    assert list(result["index"]) == [0, 1]
    expected_hash = generate_hash_business_key("CHEMBL1")
    assert result.loc[0, "hash_business_key"] == expected_hash
    assert isinstance(result.loc[0, "hash_row"], str)
    assert len(result.loc[0, "hash_row"]) == 64
    assert result.loc[0, "pipeline_version"] == "2.0.0"
    assert result.loc[0, "chembl_release"] == "ChEMBL_TEST"
    expected_order = TargetSchema.get_column_order()
    assert list(result.columns[:len(expected_order)]) == expected_order
    assert result.columns[-1] == "custom_col"


def test_align_dataframe_to_schema_orders_and_fills_missing_columns():
    df = pd.DataFrame(
        {
            "activity_id": [101],
            "standard_type": ["IC50"],
            "source_system": ["chembl"],
        }
    )

    aligned = align_dataframe_to_schema(df, ActivitySchema)
    expected_order = ActivitySchema.get_column_order()

    assert list(aligned.columns[:len(expected_order)]) == expected_order
    assert aligned.loc[0, "activity_id"] == 101
    assert aligned.loc[0, "standard_type"] == "IC50"
    assert pd.isna(aligned.loc[0, "hash_row"])


def test_align_dataframe_to_schema_appends_extra_columns_at_the_end():
    df = pd.DataFrame({"activity_id": [11], "extra_col": ["value"]})

    aligned = align_dataframe_to_schema(df, ActivitySchema)
    expected_order = ActivitySchema.get_column_order()

    assert aligned.columns[len(expected_order)] == "extra_col"
    assert aligned.shape[1] >= len(expected_order) + 1
