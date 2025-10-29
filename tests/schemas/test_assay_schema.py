"""Assay schema specific validation tests."""

from __future__ import annotations

import pandas as pd

from bioetl.schemas.assay import AssaySchema


def test_assay_schema_accepts_nullable_integer_columns() -> None:
    """Columns annotated as nullable integers should tolerate missing data."""

    schema_columns = AssaySchema.get_column_order()
    assert schema_columns, "schema column order must not be empty"

    assert "row_subtype" not in schema_columns

    row = {column: pd.NA for column in schema_columns}
    row.update(
        {
            "index": 0,
            "hash_row": "0" * 64,
            "hash_business_key": "0" * 64,
            "pipeline_version": "1.0.0",
            "source_system": "chembl",
            "chembl_release": "ChEMBL_TEST",
            "extracted_at": "2024-01-01T00:00:00+00:00",
            "assay_chembl_id": "CHEMBL1",
        }
    )

    df = pd.DataFrame([row], columns=schema_columns)

    nullable_integer_columns = [
        "assay_tax_id",
        "confidence_score",
        "src_id",
        "species_group_flag",
        "tax_id",
        "component_count",
        "assay_class_id",
        "variant_id",
    ]

    for column in nullable_integer_columns:
        if column in df.columns:
            df[column] = pd.Series(pd.array([pd.NA], dtype=pd.Int64Dtype()))

    nullable_float_columns = [
        "assay_param_value",
        "assay_param_standard_value",
        "fallback_retry_after_sec",
    ]

    for column in nullable_float_columns:
        if column in df.columns:
            df[column] = pd.Series(pd.array([pd.NA], dtype=pd.Float64Dtype()))

    validated = AssaySchema.validate(df, lazy=True)

    for column in nullable_integer_columns:
        if column in validated.columns:
            assert str(validated[column].dtype) == "Int64"
            assert validated[column].isna().all()
