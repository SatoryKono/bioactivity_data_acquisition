"""Helpers for coercing dataframe columns to deterministic dtypes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, TYPE_CHECKING, TypeAlias, cast

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from pandas._libs.missing import NAType as PandasNAType
else:  # pragma: no cover - pandas runtime provides the concrete type
    PandasNAType = type(pd.NA)

NAType: TypeAlias = PandasNAType
_DEFAULT_MINIMUM_KEYS = ("*", "__default__")


def _normalise_minimum_config(
    minimums: Mapping[str, Any] | int | float | None,
) -> tuple[dict[str, float], float | None]:
    """Return per-column and default minimum thresholds."""

    if minimums is None:
        return {}, None

    if isinstance(minimums, Mapping):
        resolved: dict[str, float] = {}
        for key, value in minimums.items():
            if value is None:
                continue
            try:
                resolved[str(key)] = float(value)
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

        min_threshold = per_column_minimums.get(column, default_minimum)
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
        numeric = pd.to_numeric(df[column], errors="coerce")
        df[column] = pd.Series(pd.array(numeric, dtype="Float64"), index=df.index)


def _coerce_optional_bool_scalar(value: object) -> bool | NAType:
    """Coerce a scalar value to a pandas ``boolean`` compatible value."""

    if value is None or (isinstance(value, float) and pd.isna(value)):
        return cast(NAType, pd.NA)

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"", "na", "none", "null"}:
            return cast(NAType, pd.NA)
        if normalized in {"true", "t", "yes", "y", "1", "on"}:
            return True
        if normalized in {"false", "f", "0", "no", "n", "off"}:
            return False
        return cast(NAType, pd.NA)

    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        return bool(value)

    if pd.isna(value):
        return cast(NAType, pd.NA)

    return bool(value)


def coerce_optional_bool(
    value: object,
    columns: Sequence[str] | None = None,
    *,
    nullable: bool = True,
) -> bool | NAType | pd.Series | pd.DataFrame | None:
    """Best-effort coercion of optional booleans preserving ``pd.NA``.

    The helper accepts several input types for backwards compatibility:

    * When ``value`` is a scalar, the coerced scalar is returned.
    * When ``value`` is a :class:`pandas.Series`, the coerced series with
      ``boolean`` dtype is returned.
    * When ``value`` is a :class:`pandas.DataFrame`, the provided ``columns``
      are coerced in-place and the dataframe is returned.

    Parameters
    ----------
    value:
        Scalar, :class:`pandas.Series` or :class:`pandas.DataFrame` to coerce.
    columns:
        Column names to coerce when ``value`` is a dataframe.
    nullable:
        When ``True`` (default) the result uses pandas' nullable ``boolean``
        dtype preserving ``pd.NA`` values. When ``False`` missing values are
        replaced with ``False`` and the result is stored using the native
        ``bool`` dtype, which is compatible with strict Pandera schemas that
        expect concrete booleans.
    """

    if isinstance(value, pd.DataFrame):
        if columns is None:
            raise TypeError(
                "'columns' must be provided when coercing a DataFrame with "
                "coerce_optional_bool."
            )
        for column in columns:
            if column not in value.columns:
                continue
            coerced = value[column].apply(_coerce_optional_bool_scalar)
            if nullable:
                value[column] = pd.Series(coerced, dtype="boolean", index=value.index)
            else:
                coerced_values = [
                    False if pd.isna(item) else bool(item) for item in coerced
                ]
                value[column] = pd.Series(
                    coerced_values, dtype="bool", index=value.index
                )
        return value

    if isinstance(value, pd.Series):
        coerced_series = value.apply(_coerce_optional_bool_scalar)
        if nullable:
            return pd.Series(coerced_series, dtype="boolean", index=value.index)
        coerced_values = [False if pd.isna(item) else bool(item) for item in coerced_series]
        return pd.Series(coerced_values, dtype="bool", index=value.index)

    coerced_scalar = _coerce_optional_bool_scalar(value)
    if nullable or isinstance(coerced_scalar, bool):
        return coerced_scalar
    return False if pd.isna(coerced_scalar) else bool(coerced_scalar)


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
    *,
    minimums: Mapping[str, Any] | None = None,
) -> None:
    """Backwards compatible wrapper around :func:`coerce_nullable_int`."""

    if minimum_values is not None and minimums is not None:
        raise TypeError(
            "'minimum_values' and 'minimums' are aliases; provide only one of them."
        )

    min_values: Mapping[str, Any] | int | float | None = (
        minimum_values if minimum_values is not None else minimums
    )
    if default_minimum is not None:
        base: dict[str, Any] = {}
        if isinstance(minimum_values, Mapping):
            base.update(minimum_values)
        elif isinstance(minimums, Mapping):
            base.update(minimums)
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
