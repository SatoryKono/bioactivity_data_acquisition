from __future__ import annotations

from collections.abc import Callable

import pandas as pd
import pandera as pa
import pytest
from pandera import Column
from pandera.errors import SchemaError, SchemaErrors
from src.bioetl.schemas.common import (
    bao_id_column,
    boolean_flag_column,
    chembl_id_column,
    doi_column,
    non_nullable_int64_column,
    nullable_float64_column,
    nullable_int64_column,
    nullable_object_column,
    nullable_pd_int64_column,
    object_column,
    row_metadata_columns,
    string_column,
    string_column_with_check,
    uuid_column,
)


@pytest.mark.parametrize(
    ("nullable", "value", "expect_error"),
    [
        (True, None, False),
        (False, None, True),
    ],
)
def test_string_column_nullable(nullable: bool, value: str | None, expect_error: bool) -> None:
    column = string_column(nullable=nullable)
    schema = pa.DataFrameSchema({"col": column})
    df = pd.DataFrame({"col": [value]})

    if expect_error:
        with pytest.raises((SchemaError, SchemaErrors)):
            schema.validate(df, lazy=True)
    else:
        schema.validate(df, lazy=True)


@pytest.mark.parametrize(
    ("unique", "values", "expect_error"),
    [
        (False, ["CHEMBL1", "CHEMBL1"], False),
        (True, ["CHEMBL1", "CHEMBL1"], True),
        (True, ["CHEMBL1", "CHEMBL2"], False),
    ],
)
def test_string_column_unique(unique: bool, values: list[str], expect_error: bool) -> None:
    column = string_column(nullable=False, unique=unique)
    schema = pa.DataFrameSchema({"col": column})
    df = pd.DataFrame({"col": values})

    if expect_error:
        with pytest.raises((SchemaError, SchemaErrors)):
            schema.validate(df, lazy=True)
    else:
        schema.validate(df, lazy=True)


@pytest.mark.parametrize(
    ("pattern", "value", "expect_error"),
    [
        (r"^CHEMBL\d+$", "CHEMBL123", False),
        (r"^CHEMBL\d+$", "INVALID", True),
    ],
)
def test_string_column_pattern(pattern: str, value: str, expect_error: bool) -> None:
    column = string_column(nullable=False, pattern=pattern)
    schema = pa.DataFrameSchema({"col": column})
    df = pd.DataFrame({"col": [value]})

    if expect_error:
        with pytest.raises((SchemaError, SchemaErrors)):
            schema.validate(df, lazy=True)
    else:
        schema.validate(df, lazy=True)


@pytest.mark.parametrize(
    ("nullable", "value", "expect_error"),
    [
        (True, None, False),
        (False, None, True),
        (False, {"a": 1}, False),
    ],
)
def test_object_column_nullable(nullable: bool, value: object, expect_error: bool) -> None:
    column = object_column(nullable=nullable)
    schema = pa.DataFrameSchema({"col": column})
    df = pd.DataFrame({"col": [value]})

    if expect_error:
        with pytest.raises((SchemaError, SchemaErrors)):
            schema.validate(df, lazy=True)
    else:
        schema.validate(df, lazy=True)


@pytest.mark.unit
def test_nullable_int64_column_constraints() -> None:
    column = nullable_int64_column(ge=0, le=10, isin={0, 5, 10})
    schema = pa.DataFrameSchema({"col": column})
    valid = pd.DataFrame({"col": pd.Series([0, 5, 10, pd.NA], dtype="Int64")})
    invalid = pd.DataFrame({"col": [11]})

    schema.validate(valid, lazy=True)
    with pytest.raises((SchemaError, SchemaErrors)):
        schema.validate(invalid, lazy=True)


@pytest.mark.unit
def test_non_nullable_int64_column_unique() -> None:
    column = non_nullable_int64_column(unique=True)
    schema = pa.DataFrameSchema({"col": column})
    valid = pd.DataFrame({"col": [1, 2, 3]})
    invalid = pd.DataFrame({"col": [1, 1]})

    schema.validate(valid, lazy=True)
    with pytest.raises((SchemaError, SchemaErrors)):
        schema.validate(invalid, lazy=True)


@pytest.mark.unit
def test_nullable_pd_int64_column_accepts_nulls() -> None:
    column = nullable_pd_int64_column(ge=0)
    schema = pa.DataFrameSchema({"value": column})
    dataframe = pd.DataFrame({"value": pd.Series([0, 1, None], dtype="Int64")})

    result = schema.validate(dataframe, lazy=True)
    assert result["value"].dtype == "Int64"


@pytest.mark.unit
def test_nullable_float64_column_bounds() -> None:
    column = nullable_float64_column(ge=0.0, le=1.0)
    schema = pa.DataFrameSchema({"value": column})
    valid = pd.DataFrame({"value": [0.0, 0.5, None]})
    invalid = pd.DataFrame({"value": [1.5]})

    schema.validate(valid, lazy=True)
    with pytest.raises((SchemaError, SchemaErrors)):
        schema.validate(invalid, lazy=True)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("use_boolean_dtype", "values", "expect_error"),
    [
        (True, pd.Series([True, False, None], dtype="boolean"), False),
        (False, pd.Series([1, 0, None], dtype="Int64"), False),
        (False, pd.Series([2], dtype="Int64"), True),
    ],
)
def test_boolean_flag_column(use_boolean_dtype: bool, values: pd.Series, expect_error: bool) -> None:
    column = boolean_flag_column(use_boolean_dtype=use_boolean_dtype)
    schema = pa.DataFrameSchema({"flag": column})
    dataframe = pd.DataFrame({"flag": values})

    if expect_error:
        with pytest.raises((SchemaError, SchemaErrors)):
            schema.validate(dataframe, lazy=True)
    else:
        schema.validate(dataframe, lazy=True)


@pytest.mark.unit
def test_string_column_with_check_combined() -> None:
    column = string_column_with_check(
        pattern=r"^ID-\d{3}$",
        isin={"ID-001", "ID-002"},
        nullable=False,
        str_length=(5, 6),
    )
    schema = pa.DataFrameSchema({"identifier": column})
    schema.validate(pd.DataFrame({"identifier": ["ID-001"]}), lazy=True)

    with pytest.raises((SchemaError, SchemaErrors)):
        schema.validate(pd.DataFrame({"identifier": ["WRONG"]}), lazy=True)


@pytest.mark.unit
def test_row_metadata_columns_factory() -> None:
    schema = pa.DataFrameSchema(row_metadata_columns())
    dataframe = pd.DataFrame({"row_subtype": ["activity"], "row_index": [0]})

    result = schema.validate(dataframe, lazy=True)
    assert list(result.columns) == ["row_subtype", "row_index"]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("factory", "value", "expect_error"),
    [
        (chembl_id_column, "CHEMBL42", False),
        (chembl_id_column, "INVALID", True),
        (bao_id_column, "BAO_1234567", False),
        (bao_id_column, "BAO_BAD", True),
        (doi_column, "10.1000/xyz", False),
        (doi_column, "doi:bad", True),
        (uuid_column, "123e4567-e89b-12d3-a456-426614174000", False),
        (uuid_column, "not-a-uuid", True),
    ],
)
def test_identifier_columns(
    factory: Callable[..., Column], value: str, expect_error: bool
) -> None:
    column = factory(nullable=False)
    schema = pa.DataFrameSchema({"identifier": column})
    dataframe = pd.DataFrame({"identifier": [value]})

    if expect_error:
        with pytest.raises((SchemaError, SchemaErrors)):
            schema.validate(dataframe, lazy=True)
    else:
        schema.validate(dataframe, lazy=True)


@pytest.mark.unit
def test_nullable_object_column_returns_nullable() -> None:
    column = nullable_object_column()

    assert column.nullable is True

