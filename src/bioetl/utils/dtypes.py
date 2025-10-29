"""Helpers for coercing dataframe columns to deterministic dtypes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd

NAType = type(pd.NA)
_DEFAULT_MINIMUM_KEYS = ("*", "__default__")


def _normalise_minimum_config(
    minimums: Mapping[str, Any] | int | float | None,
) -> tuple[dict[str, float | None], float | None]:
    """Return per-column and default minimum thresholds."""

    if minimums is None:
        return {}, None

    if isinstance(minimums, Mapping):
        resolved: dict[str, float | None] = {}
        for key, value in minimums.items():
            key_str = str(key)
            if value is None:
                resolved[key_str] = None
                continue
            try:
                resolved[key_str] = float(value)
            except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
                raise TypeError(
                    f"Minimum value for column '{key}' must be numeric, got {value!r}."
                ) from exc
        default_minimum: float | None = None
        for sentinel in _DEFAULT_MINIMUM_KEYS:
            if sentinel in resolved:
                default_minimum = resolved.pop(sentinel)
                break
        return resolved, default_minimum

    try:
        default = float(minimums)
    except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
        raise TypeError("Minimum values must be provided as a mapping or numeric scalar.") from exc
    return {}, default


def coerce_nullable_int(
    df: pd.DataFrame,
    columns: Sequence[str],
    *,
    min_values: Mapping[str, Any] | int | float | None = None,
) -> None:
    """Coerce ``columns`` to Pandas ``Int64`` dtype with optional thresholds."""

    per_column_minimums, default_minimum = _normalise_minimum_config(min_values)

    for column in columns:
        if column not in df.columns:
            continue

        series = pd.to_numeric(df[column], errors="coerce")

        if column in per_column_minimums:
            min_threshold = per_column_minimums[column]
        else:
            min_threshold = default_minimum
        if min_threshold is not None:
            below_min_mask = series < min_threshold
            if below_min_mask.any():
                series.loc[below_min_mask] = pd.NA

        non_null_mask = series.notna()
        if non_null_mask.any():
            fractional_mask = pd.Series(False, index=series.index)
            remainders = (series[non_null_mask] % 1).abs()
            fractional_mask.loc[non_null_mask] = ~np.isclose(remainders, 0.0)
            if fractional_mask.any():
                series.loc[fractional_mask] = pd.NA

        df[column] = pd.Series(pd.array(series, dtype="Int64"), index=df.index)


def coerce_nullable_float(df: pd.DataFrame, columns: Sequence[str]) -> None:
    """Coerce ``columns`` to ``float`` dtype using ``pandas.to_numeric``."""

    for column in columns:
        if column not in df.columns:
            continue
        df[column] = pd.to_numeric(df[column], errors="coerce")


def coerce_optional_bool(value: object) -> bool | NAType:
    """Best-effort coercion of optional booleans preserving ``pd.NA``."""

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

    if pd.isna(value):  # type: ignore[arg-type]
        return pd.NA

    return bool(value)


def coerce_retry_after(df: pd.DataFrame, column: str = "fallback_retry_after_sec") -> None:
    """Normalise retry-after columns to concrete ``float64`` dtype."""

    if column not in df.columns:
        return

    series = df[column]
    if not isinstance(series, pd.Series):  # pragma: no cover - defensive guard
        return

    numeric = pd.to_numeric(series, errors="coerce")
    df[column] = numeric.astype("float64", copy=False)


def coerce_nullable_int_columns(
    df: pd.DataFrame,
    columns: Sequence[str],
    minimum_values: Mapping[str, Any] | None = None,
    default_minimum: int | float | None = None,
) -> None:
    """Backwards compatible wrapper around :func:`coerce_nullable_int`."""

    min_values: Mapping[str, Any] | int | float | None = minimum_values
    if default_minimum is not None:
        base: dict[str, Any] = {}
        if isinstance(minimum_values, Mapping):
            base.update(minimum_values)
        base[_DEFAULT_MINIMUM_KEYS[0]] = default_minimum
        min_values = base
    coerce_nullable_int(df, columns, min_values=min_values)


def coerce_nullable_float_columns(df: pd.DataFrame, columns: Sequence[str]) -> None:
    """Backwards compatible wrapper around :func:`coerce_nullable_float`."""

    coerce_nullable_float(df, columns)


__all__ = [
    "NAType",
    "coerce_nullable_float",
    "coerce_nullable_float_columns",
    "coerce_nullable_int",
    "coerce_nullable_int_columns",
    "coerce_optional_bool",
    "coerce_retry_after",
]
