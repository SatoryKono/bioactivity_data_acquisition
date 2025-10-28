"""Helpers for deterministic dataframe post-processing."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Sequence, TypeVar

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
        expected_columns = schema.get_column_order()

    if expected_columns:
        # Ensure all expected columns exist
        for column in expected_columns:
            if column not in result.columns:
                result[column] = pd.NA
        extras = [col for col in result.columns if col not in expected_columns]
        result = result[list(expected_columns) + extras]

    return result.convert_dtypes()
