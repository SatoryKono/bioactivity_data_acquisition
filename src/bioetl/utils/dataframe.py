"""Helpers for deterministic dataframe post-processing."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from typing import TypeVar

import pandas as pd

from bioetl.core.hashing import generate_hash_business_key, generate_hash_row
from bioetl.schemas.base import BaseSchema

DataFrameT = TypeVar("DataFrameT", bound=pd.DataFrame)


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
    schema: type[BaseSchema] | None = None,
) -> DataFrameT:
    """Apply deterministic metadata, hashing and ordering to a dataframe."""

    if df.empty:
        return df.copy()

    result = df.copy()

    if pipeline_version is None:
        pipeline_version = "1.0.0"
    result["pipeline_version"] = pipeline_version

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
    result["hash_row"] = result.apply(lambda row: generate_hash_row(row.to_dict()), axis=1)

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
