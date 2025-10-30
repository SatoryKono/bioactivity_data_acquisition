"""Helpers for deterministic dataframe post-processing.

This module focuses on deterministic augmentation of dataframe outputs.  The
hashing logic in :func:`finalize_pipeline_output` relies on
``pandas.util.hash_pandas_object`` so that row level hashes are computed without
row-wise ``DataFrame.apply`` calls.  The resulting SHA256 digests are stable
because the hashing input is derived from a sorted projection of the dataframe
columns.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, TypeVar

import pandas as pd
from pandas.util import hash_pandas_object

from bioetl.core.hashing import generate_hash_business_key
from bioetl.schemas.base import BaseSchema

DataFrameT = TypeVar("DataFrameT", bound=pd.DataFrame)


def _normalise_hashable(value: Any) -> Any:
    """Coerce nested containers to hash-friendly immutable equivalents."""

    if value is None or value is pd.NA:
        return value

    if isinstance(value, Mapping):
        return tuple(
            (str(key), _normalise_hashable(item))
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        )

    if isinstance(value, (list, tuple)):
        return tuple(_normalise_hashable(item) for item in value)

    if isinstance(value, set):
        return tuple(sorted(_normalise_hashable(item) for item in value))

    if isinstance(value, pd.Series):
        return tuple(_normalise_hashable(item) for item in value.tolist())

    if isinstance(value, pd.Index):
        return tuple(_normalise_hashable(item) for item in value.tolist())

    if isinstance(value, pd.DataFrame):  # pragma: no cover - defensive guard
        return tuple(
            (
                str(column),
                tuple(_normalise_hashable(item) for item in value[column].tolist()),
            )
            for column in value.columns
        )

    try:  # numpy is an optional dependency at runtime
        import numpy as np

        if isinstance(value, np.ndarray):
            return tuple(_normalise_hashable(item) for item in value.tolist())
    except Exception:  # pragma: no cover - guard for environments without numpy
        pass

    return value


def _requires_normalisation(series: pd.Series) -> bool:
    """Return True if the series contains containers that need coercion."""

    if series.dtype != "object":
        return False

    sample = series.dropna().head(5)
    if sample.empty:
        return False

    container_types: tuple[type[Any], ...] = (list, tuple, set, dict, pd.Series, pd.Index)

    try:
        import numpy as np

        container_types = container_types + (np.ndarray,)
    except Exception:  # pragma: no cover - numpy optional in typing contexts
        pass

    return any(isinstance(value, container_types) for value in sample)


def _ensure_sequence(values: Sequence[bool] | bool | None, length: int) -> list[bool]:
    """Normalize ascending flags to a list with the given length."""

    if length == 0:
        return []

    if values is None:
        return [True] * length

    if isinstance(values, bool):
        return [values] * length

    values_list = list(values)
    if len(values_list) != length:
        raise ValueError(
            "Length of ascending sequence must match sort columns"
        )
    return values_list


def resolve_schema_column_order(schema: type[BaseSchema] | None) -> list[str]:
    """Return the canonical column order for a schema.

    Pandera's :class:`~pandera.api.pandas.model.DataFrameModel` exposes a
    ``get_column_order`` helper in our ``BaseSchema`` subclasses, but in
    practice some schemas may not populate ``_column_order`` (for example when
    the contract was imported from an external source).  Historically this led
    to callers receiving an empty list, skipping reordering logic and letting
    Pandera enforce its own field order.  On certain environments this surfaced
    as ``column '...' out-of-order`` validation failures.

    This helper normalises the behaviour by preferring the explicit
    ``get_column_order`` value and falling back to the concrete DataFrameSchema
    definition so that callers can deterministically align their dataframe
    columns with the schema contract.
    """

    if schema is None:
        return []

    try:
        explicit_order = schema.get_column_order()
    except AttributeError:
        explicit_order = []

    if explicit_order:
        return list(explicit_order)

    fallback_order = getattr(schema, "_column_order", None)
    if fallback_order:
        return list(fallback_order)

    try:
        materialised = schema.to_schema()
    except Exception:  # pragma: no cover - defensive fallback
        materialised = None

    if materialised is not None:
        try:
            columns = list(materialised.columns.keys())
        except AttributeError:  # pragma: no cover - legacy Pandera versions
            columns = list(materialised.columns)
        if columns:
            return columns

    # Final fallback: rely on Pydantic's field order if available.
    model_fields = getattr(schema, "model_fields", None)
    if isinstance(model_fields, dict) and model_fields:
        return list(model_fields.keys())

    fields = getattr(schema, "__fields__", None)
    if isinstance(fields, dict) and fields:
        return list(fields.keys())

    return []


def finalize_pipeline_output(
    df: DataFrameT,
    *,
    business_key: str,
    sort_by: Sequence[str] | None = None,
    ascending: Sequence[bool] | bool | None = None,
    pipeline_version: str | None = None,
    source_system: str | None = None,
    chembl_release: str | None = None,
    extracted_at: str | None = None,
    run_id: str | None = None,
    schema: type[BaseSchema] | None = None,
) -> DataFrameT:
    """Apply deterministic metadata, hashing and ordering to a dataframe.

    The helper enriches the dataframe with metadata columns (creating optional
    fields such as ``run_id`` when absent), calculates the ``hash_business_key``
    and ``hash_row`` digests and finally enforces a canonical ordering.
    ``hash_row`` values are generated via
    :func:`pandas.util.hash_pandas_object` on the dataframe columns sorted by
    name which guarantees deterministic SHA256 digests regardless of platform or
    dataframe row ordering.
    """

    if df.empty:
        return df.copy()

    result = df.copy()

    if pipeline_version is None:
        pipeline_version = "1.0.0"
    result["pipeline_version"] = pipeline_version

    if run_id is not None:
        result["run_id"] = run_id
    else:
        if "run_id" not in result.columns:
            result["run_id"] = pd.Series(pd.NA, index=result.index, dtype="string")
        elif result["run_id"].isna().any():
            raise ValueError("run_id column contains null values")

    if source_system is not None:
        result["source_system"] = source_system
    elif "source_system" not in result.columns:
        result["source_system"] = "chembl"

    if chembl_release is not None:
        result["chembl_release"] = chembl_release
    elif "chembl_release" not in result.columns:
        result["chembl_release"] = pd.NA

    if extracted_at is None:
        extracted_at = datetime.now(timezone.utc).isoformat()
    result["extracted_at"] = extracted_at

    if business_key not in result.columns:
        raise KeyError(f"business key column '{business_key}' is missing")

    result["hash_business_key"] = result[business_key].apply(generate_hash_business_key)

    hash_source_columns = sorted(result.columns)
    hash_frame = result[hash_source_columns].copy()

    for column in hash_frame.columns:
        series = hash_frame[column]
        if _requires_normalisation(series):
            hash_frame[column] = series.map(_normalise_hashable)

    hash_vector = hash_pandas_object(hash_frame, index=False)

    hash_bytes = hash_vector.to_numpy(dtype="uint64", copy=False)
    row_hashes = [
        hashlib.sha256(int(value).to_bytes(8, byteorder="little", signed=False)).hexdigest()
        for value in hash_bytes
    ]
    result["hash_row"] = pd.Series(row_hashes, index=result.index, dtype="string")

    if sort_by:
        sort_columns = [column for column in sort_by if column in result.columns]
        if sort_columns:
            sort_flags = _ensure_sequence(ascending, len(sort_columns))
            result = result.sort_values(sort_columns, ascending=sort_flags, kind="stable")

    result = result.reset_index(drop=True)
    result["index"] = range(len(result))

    expected_columns: Iterable[str] = []
    if schema is not None:
        expected_columns = resolve_schema_column_order(schema)

    if expected_columns:
        # Preserve original order for any additional columns while ensuring the
        # schema-defined ones appear first.
        extra_columns = [column for column in result.columns if column not in expected_columns]

        for column in expected_columns:
            if column not in result.columns:
                result[column] = pd.NA

        ordered_columns = [column for column in expected_columns if column in result.columns]
        result = result[ordered_columns + extra_columns]

    return result.convert_dtypes()
