"""Утилиты для приведения типов данных."""

from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd

NAType = type(pd.NA)


def coerce_nullable_int_columns(
    df: pd.DataFrame,
    columns: Sequence[str],
    minimum_values: Mapping[str, int] | None = None,
    default_minimum: int | None = None,
) -> None:
    """Приводит колонки к nullable Int64 dtype с проверкой минимумов.

    Объединяет логику из activity.py, assay.py и testitem.py:
    - Приведение к numeric через pd.to_numeric(..., errors="coerce")
    - Маскировка значений ниже минимума
    - Маскировка дробных значений через series % 1
    - Приведение к Int64 dtype

    Args:
        df: DataFrame для обработки
        columns: Список колонок для приведения
        minimum_values: Словарь минимальных значений для конкретных колонок
        default_minimum: Минимальное значение по умолчанию для всех колонок
    """
    for column in columns:
        if column not in df.columns:
            continue

        series = pd.to_numeric(df[column], errors="coerce")

        # Применяем минимальные значения
        min_value = None
        if minimum_values is not None and column in minimum_values:
            min_value = minimum_values[column]
        elif default_minimum is not None:
            min_value = default_minimum

        if min_value is not None:
            below_min_mask = series < min_value
            if below_min_mask.any():
                series.loc[below_min_mask] = pd.NA

        if series.empty:
            df[column] = pd.Series(pd.array(series, dtype="Int64"), index=df.index)
            continue

        # Маскируем дробные значения
        non_null = series.notna()
        fractional_mask = pd.Series(False, index=series.index)
        if non_null.any():
            remainders = (series[non_null] % 1).abs()
            fractional_mask.loc[non_null] = ~np.isclose(remainders, 0.0)

        if fractional_mask.any():
            series.loc[fractional_mask] = pd.NA

        df[column] = pd.Series(pd.array(series, dtype="Int64"), index=df.index)


def coerce_nullable_float_columns(df: pd.DataFrame, columns: Sequence[str]) -> None:
    """Приводит колонки к float64 с NaN placeholder'ами.

    Args:
        df: DataFrame для обработки
        columns: Список колонок для приведения
    """
    for column in columns:
        if column not in df.columns:
            continue
        df[column] = pd.to_numeric(df[column], errors="coerce")


def coerce_optional_bool(value: object) -> bool | NAType:
    """Мягкое приведение к nullable boolean с pd.NA.

    Из document.py - обрабатывает различные представления булевых значений,
    возвращая pd.NA для неопределённых случаев.

    Args:
        value: Значение для приведения

    Returns:
        bool, pd.NA или bool
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return pd.NA

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"", "na", "none", "null"}:
            return pd.NA
        if normalized in {"true", "1", "yes", "y", "t"}:
            return True
        if normalized in {"false", "0", "no", "n", "f"}:
            return False

    if isinstance(value, (bool, int)):
        return bool(value)

    if pd.isna(value):
        return pd.NA

    return bool(value)
