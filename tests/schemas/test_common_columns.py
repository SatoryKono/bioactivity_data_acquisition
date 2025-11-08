from __future__ import annotations

import pandas as pd
import pandera as pa
import pytest

from src.bioetl.schemas.common import object_column, string_column


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
        with pytest.raises(pa.errors.SchemaError):
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
        with pytest.raises(pa.errors.SchemaError):
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
        with pytest.raises(pa.errors.SchemaError):
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
        with pytest.raises(pa.errors.SchemaError):
            schema.validate(df, lazy=True)
    else:
        schema.validate(df, lazy=True)

