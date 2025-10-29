"""Тесты для утилит dtype."""

import pandas as pd

from bioetl.utils.dtype import (
    coerce_nullable_float_columns,
    coerce_nullable_int_columns,
    coerce_optional_bool,
)


class TestCoerceNullableIntColumns:
    """Тесты для coerce_nullable_int_columns."""

    def test_basic_coercion(self):
"""Тест базового приведения к Int64."""
df = pd.DataFrame({
    "col1": ["1", "2", "3"],
    "col2": [1.0, 2.0, 3.0],
    "col3": [1, 2, 3],
})
coerce_nullable_int_columns(df, ["col1", "col2", "col3"])

assert df["col1"].dtype == "Int64"
assert df["col2"].dtype == "Int64"
assert df["col3"].dtype == "Int64"
assert df["col1"].tolist() == [1, 2, 3]
assert df["col2"].tolist() == [1, 2, 3]
assert df["col3"].tolist() == [1, 2, 3]

    def test_fractional_values(self):
"""Тест обработки дробных значений."""
df = pd.DataFrame({
    "col1": [1.0, 1.5, 2.0, 2.7],
})
coerce_nullable_int_columns(df, ["col1"])
assert df["col1"].dtype == "Int64"
# Дробные значения должны стать <NA>
assert df["col1"].isna().tolist() == [False, True, False, True]
assert df["col1"].dropna().tolist() == [1, 2]

    def test_na_values(self):
"""Тест обработки NA значений."""
df = pd.DataFrame({
    "col1": [1, None, pd.NA, "3"],
    "col2": ["", "2", "abc", "4"],
})
coerce_nullable_int_columns(df, ["col1", "col2"])
assert df["col1"].dtype == "Int64"
assert df["col2"].dtype == "Int64"
assert df["col1"].isna().tolist() == [False, True, True, False]
assert df["col2"].isna().tolist() == [True, False, True, False]

    def test_minimum_values(self):
"""Тест проверки минимальных значений."""
df = pd.DataFrame({
    "col1": [1, 2, 3, 4, 5],
    "col2": [0, 1, 2, 3, 4],
})
coerce_nullable_int_columns(
    df, 
    ["col1", "col2"],
    minimum_values={"col1": 2, "col2": 1},
    default_minimum=0
)
assert df["col1"].dtype == "Int64"
assert df["col2"].dtype == "Int64"
# col1: значения < 2 должны стать <NA>
assert df["col1"].isna().tolist() == [True, False, False, False, False]
# col2: значения < 1 должны стать <NA>
assert df["col2"].isna().tolist() == [True, False, False, False, False]

    def test_missing_columns(self):
"""Тест обработки отсутствующих колонок."""
df = pd.DataFrame({"col1": [1, 2, 3]})
# Не должно вызывать ошибку
coerce_nullable_int_columns(df, ["col1", "missing_col"])
assert df["col1"].dtype == "Int64"
assert "missing_col" not in df.columns

    def test_empty_dataframe(self):
"""Тест обработки пустого DataFrame."""
df = pd.DataFrame({"col1": []})
coerce_nullable_int_columns(df, ["col1"])
assert df["col1"].dtype == "Int64"
assert len(df) == 0


class TestCoerceNullableFloatColumns:
    """Тесты для coerce_nullable_float_columns."""

    def test_basic_coercion(self):
"""Тест базового приведения к float64."""
df = pd.DataFrame({
    "col1": ["1.5", "2.7", "3.0"],
    "col2": [1, 2, 3],
    "col3": [1.5, 2.7, 3.0],
})
coerce_nullable_float_columns(df, ["col1", "col2", "col3"])
assert df["col1"].dtype == "float64"
# col2 остается int64, так как содержит целые числа
assert df["col2"].dtype == "int64"
assert df["col3"].dtype == "float64"
assert df["col1"].tolist() == [1.5, 2.7, 3.0]
assert df["col2"].tolist() == [1.0, 2.0, 3.0]
assert df["col3"].tolist() == [1.5, 2.7, 3.0]

    def test_invalid_values(self):
"""Тест обработки невалидных значений."""
df = pd.DataFrame({
    "col1": ["1.5", "abc", "3.0", ""],
})
coerce_nullable_float_columns(df, ["col1"])
assert df["col1"].dtype == "float64"
# Невалидные значения должны стать NaN
assert pd.isna(df["col1"].iloc[1])  # "abc"
assert pd.isna(df["col1"].iloc[3])  # ""

    def test_missing_columns(self):
"""Тест обработки отсутствующих колонок."""
df = pd.DataFrame({"col1": [1.5, 2.7]})
# Не должно вызывать ошибку
coerce_nullable_float_columns(df, ["col1", "missing_col"])
assert df["col1"].dtype == "float64"
assert "missing_col" not in df.columns


class TestCoerceOptionalBool:
    """Тесты для coerce_optional_bool."""

    def test_true_values(self):
"""Тест True значений."""
assert coerce_optional_bool(True) is True
assert coerce_optional_bool(1) is True
assert coerce_optional_bool("true") is True
assert coerce_optional_bool("1") is True
assert coerce_optional_bool("yes") is True
assert coerce_optional_bool("y") is True
assert coerce_optional_bool("t") is True

    def test_false_values(self):
"""Тест False значений."""
assert coerce_optional_bool(False) is False
assert coerce_optional_bool(0) is False
assert coerce_optional_bool("false") is False
assert coerce_optional_bool("0") is False
assert coerce_optional_bool("no") is False
assert coerce_optional_bool("n") is False
assert coerce_optional_bool("f") is False

    def test_na_values(self):
"""Тест NA значений."""
assert coerce_optional_bool(None) is pd.NA
assert coerce_optional_bool("") is pd.NA
assert coerce_optional_bool("na") is pd.NA
assert coerce_optional_bool("none") is pd.NA
assert coerce_optional_bool("null") is pd.NA
assert coerce_optional_bool(pd.NA) is pd.NA
assert coerce_optional_bool(pd.NaT) is pd.NA

    def test_float_na_values(self):
"""Тест float NA значений."""
assert coerce_optional_bool(float("nan")) is pd.NA
# pd.NaType() не существует в новых версиях pandas
# assert coerce_optional_bool(pd.NaType()) is pd.NA

    def test_other_values(self):
"""Тест других значений."""
# Неизвестные строки становятся True (bool("unknown") = True)
assert coerce_optional_bool("unknown") is True
assert coerce_optional_bool("xyz") is True
# Числа > 0 должны стать True
assert coerce_optional_bool(2) is True
assert coerce_optional_bool(3.14) is True
assert coerce_optional_bool(-1) is True
