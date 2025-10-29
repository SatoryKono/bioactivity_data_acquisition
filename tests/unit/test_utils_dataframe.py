"""Tests for dataframe utility helpers."""

import pandera.pandas as pa
from pandera.typing import Series

import pandas as pd

from bioetl.core.hashing import generate_hash_business_key
from bioetl.schemas import TargetSchema
from bioetl.schemas.base import BaseSchema
from bioetl.utils import (
    align_dataframe_columns,
    finalize_pipeline_output,
    resolve_schema_column_order,
)


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


def test_resolve_schema_column_order_prefers_explicit_order():
    """Explicit column order should be respected when provided."""

    class DemoSchema(BaseSchema):
        foo: Series[int] = pa.Field()

        class Config(BaseSchema.Config):
            ordered = True

    DemoSchema._column_order = [
        "index",
        "hash_row",
        "hash_business_key",
        "pipeline_version",
        "source_system",
        "chembl_release",
        "extracted_at",
        "foo",
    ]

    assert resolve_schema_column_order(DemoSchema) == DemoSchema._column_order


def test_resolve_schema_column_order_fallbacks_to_materialised_schema():
    """When no explicit order is defined the Pandera schema order is used."""

    class NoOrderSchema(BaseSchema):
        bar: Series[int] = pa.Field()

        class Config(BaseSchema.Config):
            ordered = True

    # Ensure _column_order is missing to exercise the fallback branch.
    if hasattr(NoOrderSchema, "_column_order"):
        delattr(NoOrderSchema, "_column_order")

    order = resolve_schema_column_order(NoOrderSchema)

    assert order[:7] == [
        "index",
        "hash_row",
        "hash_business_key",
        "pipeline_version",
        "source_system",
        "chembl_release",
        "extracted_at",
    ]
    assert order[7] == "bar"


def test_align_dataframe_columns_adds_missing_and_preserves_extras():
    """Dataframes should be realigned to schema order without losing extra fields."""

    class DemoSchema(BaseSchema):
        foo: Series[int] = pa.Field()
        bar: Series[str] = pa.Field(nullable=True)

        class Config(BaseSchema.Config):
            ordered = True

    DemoSchema._column_order = [
        "index",
        "hash_row",
        "hash_business_key",
        "pipeline_version",
        "source_system",
        "chembl_release",
        "extracted_at",
        "foo",
        "bar",
    ]

    df = pd.DataFrame(
        [
            {"bar": "b", "foo": 1, "extra": "x"},
        ],
        columns=["bar", "foo", "extra"],
    )

    aligned = align_dataframe_columns(df, DemoSchema)

    expected_order = DemoSchema._column_order
    assert list(aligned.columns[: len(expected_order)]) == expected_order
    assert list(aligned.columns[len(expected_order) :]) == ["extra"]
    assert aligned.loc[0, "bar"] == "b"
    assert pd.isna(aligned.loc[0, "hash_row"])  # added column defaults to NA
