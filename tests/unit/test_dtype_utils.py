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

    def test_minimum_values(self):
        """Тест обработки минимальных значений."""
        df = pd.DataFrame({
            "col1": [0, 1, 2, 3],
            "col2": [-1, 0, 1, 2],
        })

        coerce_nullable_int_columns(df, ["col1", "col2"], minimums={"col1": 2, "col2": 1})

        assert df["col1"].dtype == "Int64"
        assert df["col2"].dtype == "Int64"
        # col1: значения < 2 должны стать <NA>
        assert df["col1"].isna().tolist() == [True, True, False, False]
        # col2: значения < 1 должны стать <NA>
        assert df["col2"].isna().tolist() == [True, True, False, False]

    def test_none_values(self):
        """Тест обработки None значений."""
        df = pd.DataFrame({
            "col1": [1, None, 3, None],
            "col2": [None, 2, None, 4],
        })

        coerce_nullable_int_columns(df, ["col1", "col2"])

        assert df["col1"].dtype == "Int64"
        assert df["col2"].dtype == "Int64"
        assert df["col1"].isna().tolist() == [False, True, False, True]
        assert df["col2"].isna().tolist() == [True, False, True, False]

    def test_string_values(self):
        """Тест обработки строковых значений."""
        df = pd.DataFrame({
            "col1": ["1", "2", "abc", "4"],
            "col2": ["1.0", "2.5", "3", "def"],
        })

        coerce_nullable_int_columns(df, ["col1", "col2"])

        assert df["col1"].dtype == "Int64"
        assert df["col2"].dtype == "Int64"
        # Невалидные строки должны стать <NA>
        assert df["col1"].isna().tolist() == [False, False, True, False]
        assert df["col2"].isna().tolist() == [False, True, False, True]


class TestCoerceNullableFloatColumns:
    """Тесты для coerce_nullable_float_columns."""

    def test_basic_coercion(self):
        """Тест базового приведения к Float64."""
        df = pd.DataFrame({
            "col1": ["1.5", "2.7", "3.0"],
            "col2": [1.0, 2.0, 3.0],
            "col3": [1, 2, 3],
        })

        coerce_nullable_float_columns(df, ["col1", "col2", "col3"])

        assert df["col1"].dtype == "Float64"
        assert df["col2"].dtype == "Float64"
        assert df["col3"].dtype == "Float64"
        assert df["col1"].tolist() == [1.5, 2.7, 3.0]
        assert df["col2"].tolist() == [1.0, 2.0, 3.0]
        assert df["col3"].tolist() == [1.0, 2.0, 3.0]

    def test_none_values(self):
        """Тест обработки None значений."""
        df = pd.DataFrame({
            "col1": [1.5, None, 3.0, None],
            "col2": [None, 2.5, None, 4.0],
        })

        coerce_nullable_float_columns(df, ["col1", "col2"])

        assert df["col1"].dtype == "Float64"
        assert df["col2"].dtype == "Float64"
        assert df["col1"].isna().tolist() == [False, True, False, True]
        assert df["col2"].isna().tolist() == [True, False, True, False]

    def test_string_values(self):
        """Тест обработки строковых значений."""
        df = pd.DataFrame({
            "col1": ["1.5", "2.7", "abc", "4.0"],
            "col2": ["1", "2.5", "3", "def"],
        })

        coerce_nullable_float_columns(df, ["col1", "col2"])

        assert df["col1"].dtype == "Float64"
        assert df["col2"].dtype == "Float64"
        # Невалидные строки должны стать <NA>
        assert df["col1"].isna().tolist() == [False, False, True, False]
        assert df["col2"].isna().tolist() == [False, False, False, True]


class TestCoerceOptionalBool:
    """Тесты для coerce_optional_bool."""

    def test_basic_coercion(self):
        """Тест базового приведения к boolean."""
        df = pd.DataFrame({
            "col1": [True, False, True, False],
            "col2": [1, 0, 1, 0],
            "col3": ["true", "false", "True", "False"],
        })

        coerce_optional_bool(df, ["col1", "col2", "col3"])

        assert df["col1"].dtype == "boolean"
        assert df["col2"].dtype == "boolean"
        assert df["col3"].dtype == "boolean"
        assert df["col1"].tolist() == [True, False, True, False]
        assert df["col2"].tolist() == [True, False, True, False]
        assert df["col3"].tolist() == [True, False, True, False]

    def test_none_values(self):
        """Тест обработки None значений."""
        df = pd.DataFrame({
            "col1": [True, None, False, None],
            "col2": [None, True, None, False],
        })

        coerce_optional_bool(df, ["col1", "col2"])

        assert df["col1"].dtype == "boolean"
        assert df["col2"].dtype == "boolean"
        assert df["col1"].isna().tolist() == [False, True, False, True]
        assert df["col2"].isna().tolist() == [True, False, True, False]

    def test_string_values(self):
        """Тест обработки строковых значений."""
        df = pd.DataFrame({
            "col1": ["true", "false", "yes", "no"],
            "col2": ["1", "0", "on", "off"],
        })

        coerce_optional_bool(df, ["col1", "col2"])

        assert df["col1"].dtype == "boolean"
        assert df["col2"].dtype == "boolean"
        assert df["col1"].tolist() == [True, False, True, False]
        assert df["col2"].tolist() == [True, False, True, False]

    def test_nullable_false_dataframe(self):
        """Ненулевые значения должны конвертироваться в bool без NA."""

        df = pd.DataFrame({
            "flag": ["true", None, "false", "1"],
            "fallback": [pd.NA, "no", "yes", 0],
        })

        coerce_optional_bool(df, ["flag", "fallback"], nullable=False)

        assert df["flag"].dtype == bool
        assert df["fallback"].dtype == bool
        assert df["flag"].tolist() == [True, False, False, True]
        assert df["fallback"].tolist() == [False, False, True, False]

    def test_nullable_false_series(self):
        """Серии с NA приводятся к bool при nullable=False."""

        series = pd.Series(["true", None, "false"], dtype="object")
        result = coerce_optional_bool(series, nullable=False)

        assert result.dtype == bool
        assert result.tolist() == [True, False, False]

    def test_nullable_false_scalar(self):
        """Скалярные значения корректно обрабатываются при nullable=False."""

        assert coerce_optional_bool(None, nullable=False) is False
        assert coerce_optional_bool("true", nullable=False) is True
