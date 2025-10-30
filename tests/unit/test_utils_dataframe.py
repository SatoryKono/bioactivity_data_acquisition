"""Tests for dataframe utility helpers."""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series

from bioetl.core.hashing import generate_hash_business_key
from bioetl.schemas import TargetSchema
from bioetl.schemas.base import BaseSchema
from bioetl.utils import finalize_pipeline_output, resolve_schema_column_order


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


def test_finalize_pipeline_output_handles_nested_iterables():
    """Hash computation should tolerate list, dict and set values."""

    df = pd.DataFrame(
        [
            {
                "target_chembl_id": "CHEMBL3",
                "authors": ["Alice", "Bob"],
                "metrics": {"citations": 3, "year": 2024},
                "tags": {"oa", "biology"},
            },
            {
                "target_chembl_id": "CHEMBL4",
                "authors": ["Carol"],
                "metrics": {"citations": 5, "year": 2023},
                "tags": {"chemistry"},
            },
        ]
    )

    result = finalize_pipeline_output(
        df,
        business_key="target_chembl_id",
        sort_by=["target_chembl_id"],
        ascending=True,
        pipeline_version="1.0.0",
        source_system="chembl",
        chembl_release="ChEMBL_TEST",
        extracted_at="2024-01-01T00:00:00+00:00",
        run_id="demo-run",
    )

    assert list(result["target_chembl_id"]) == ["CHEMBL3", "CHEMBL4"]
    assert result["hash_row"].notna().all()
    # Ensure original list/dict/set structures are preserved after hashing
    assert result.loc[0, "authors"] == ["Alice", "Bob"]
    assert result.loc[0, "metrics"] == {"citations": 3, "year": 2024}
    assert result.loc[0, "tags"] == {"oa", "biology"}


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
