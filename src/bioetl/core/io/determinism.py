"""Deterministic dataframe preparation helpers (domain layer)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd

from bioetl.config.models.models import PipelineConfig

from .hashing import hash_from_mapping

CSVQuotingLiteral = Literal[0, 1, 2, 3]

__all__ = [
    "CSVQuotingLiteral",
    "DeterministicWriteArtifacts",
    "build_write_artifacts",
    "ensure_hash_columns",
    "prepare_dataframe",
    "serialise_metadata",
]


@dataclass(frozen=True, slots=True)
class DeterministicWriteArtifacts:
    """In-memory view of a dataset prepared for writing."""

    dataframe: pd.DataFrame
    metadata: Mapping[str, Any]


def _compute_hash_column(
    df: pd.DataFrame,
    *,
    fields: list[str],
    algorithm: str,
) -> pd.Series:
    records: list[dict[str, Any]] = []
    for tuple_values in df[fields].itertuples(index=False, name=None):
        record = dict(zip(fields, tuple_values, strict=True))
        records.append(record)
    hashes = [
        hash_from_mapping(record, fields, algorithm=algorithm)
        for record in records
    ]
    return pd.Series(hashes, index=df.index, dtype="string")


def ensure_hash_columns(
    df: pd.DataFrame, *, config: PipelineConfig
) -> pd.DataFrame:
    """Return ``df`` with integrity hash columns populated."""

    # Early return for empty DataFrame - no need to compute hashes
    if df.empty:
        result = df.copy()
        hashing_config = config.determinism.hashing
        row_column = hashing_config.row_hash_column
        business_column = hashing_config.business_key_column
        # Initialize hash columns with empty series if they don't exist
        if row_column not in result.columns:
            result[row_column] = pd.Series(dtype="string")
        if business_column and business_column not in result.columns:
            result[business_column] = pd.Series(dtype="string")
        return result

    hashing_config = config.determinism.hashing
    exclude = set(hashing_config.exclude_fields)
    algorithm = hashing_config.algorithm
    row_column = hashing_config.row_hash_column
    business_column = hashing_config.business_key_column

    if hashing_config.row_fields:
        row_fields = list(hashing_config.row_fields)
    else:
        row_fields = [col for col in df.columns if col not in exclude]

    missing_row_fields = [
        field for field in row_fields if field not in df.columns
    ]
    if missing_row_fields:
        missing_str = ", ".join(missing_row_fields)
        raise KeyError(f"Field(s) {missing_str} is missing from dataframe")

    business_fields = list(hashing_config.business_key_fields)
    missing_business_fields = [
        field for field in business_fields if field not in df.columns
    ]
    if missing_business_fields:
        missing_str = ", ".join(missing_business_fields)
        raise KeyError(f"Field(s) {missing_str} is missing from dataframe")

    result = df.copy()

    def _needs_recompute(series: pd.Series) -> bool:
        if series.empty:
            return True
        as_string = series.astype("string")
        return bool(
            as_string.isna().any() or (as_string.str.strip() == "").any()
        )

    if row_column in result.columns:
        row_needs_recompute = _needs_recompute(result[row_column])
    else:
        row_needs_recompute = True

    if row_needs_recompute:
        row_fields = list(row_fields)  # ensure deterministic ordering
        result[row_column] = _compute_hash_column(
            result,
            fields=row_fields,
            algorithm=algorithm,
        )

    if business_fields:
        if business_column in result.columns:
            business_needs_recompute = _needs_recompute(
                result[business_column]
            )
        else:
            business_needs_recompute = True
        if business_needs_recompute:
            result[business_column] = _compute_hash_column(
                result,
                fields=business_fields,
                algorithm=algorithm,
            )

    return result


def _stable_sort(df: pd.DataFrame, *, config: PipelineConfig) -> pd.DataFrame:
    sort_config = config.determinism.sort
    if not sort_config.by:
        return df

    # Check if all sort columns exist in the DataFrame
    missing_columns = [col for col in sort_config.by if col not in df.columns]

    # If DataFrame is empty and columns are missing, skip sorting (empty DataFrame)
    if df.empty and missing_columns:
        return df

    # If DataFrame is not empty but columns are missing, raise error
    if missing_columns:
        msg = f"Sort columns missing from dataframe: {missing_columns}"
        raise KeyError(msg)

    ascending_list: list[bool] = (
        list(sort_config.ascending)
        if sort_config.ascending
        else [True] * len(sort_config.by)
    )
    if sort_config.na_position in ("first", "last"):
        na_pos_str = sort_config.na_position
    else:
        na_pos_str = "last"
    na_pos: Literal["first", "last"] = cast(
        Literal["first", "last"],
        na_pos_str,
    )
    return df.sort_values(
        by=sort_config.by,
        ascending=ascending_list,
        na_position=na_pos,
        kind="stable",
    ).reset_index(drop=True)


def _enforce_column_order(
    df: pd.DataFrame, *, config: PipelineConfig
) -> pd.DataFrame:
    order = list(config.determinism.column_order)
    if not order:
        return df
    missing = [column for column in order if column not in df.columns]
    if missing:
        msg = f"Column order references missing columns: {missing}"
        raise ValueError(msg)
    extra = [column for column in df.columns if column not in order]
    return df[[*order, *extra]]


def prepare_dataframe(
    df: pd.DataFrame, *, config: PipelineConfig
) -> pd.DataFrame:
    """Apply determinism rules (column order + sort) to ``df``."""

    ordered = _enforce_column_order(df, config=config)
    return _stable_sort(ordered, config=config)


def serialise_metadata(
    df: pd.DataFrame,
    *,
    config: PipelineConfig,
    run_id: str,
    pipeline_code: str,
    dataset_path: Path,
    stage_durations_ms: Mapping[str, float],
) -> dict[str, Any]:
    """Serialise metadata for ``df`` using ``config`` and runtime context."""

    hashing = config.determinism.hashing
    serialized_sort = config.determinism.sort.model_dump(mode="json")
    serialized_validation = config.validation.model_dump(mode="json")

    base_metadata: dict[str, Any] = {
        "pipeline": pipeline_code,
        "run_id": run_id,
        "config_version": config.version,
        "pipeline_version": config.pipeline.version,
        "hash_policy_version": config.determinism.hash_policy_version,
        "row_count": int(df.shape[0]),
        "generated_at_utc": datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        # Canonical column order for the final dataset
        "columns": list(df.columns),
        # Deterministic dataset location
        "dataset_path": dataset_path.as_posix(),
        # Stable sort policy at top level for compatibility with golden meta
        "sorting": serialized_sort,
        "validation": serialized_validation,
        # Stage durations in milliseconds
        "stage_durations_ms": dict(stage_durations_ms),
        # Hashing section will be populated with both policy and aggregate metrics
        "hashing": {},
    }

    hashing_column_meta: dict[str, Any] = {}
    if hashing.row_hash_column in df.columns:
        column = df[hashing.row_hash_column]
        hashing_column_meta[hashing.row_hash_column] = {
            "unique": int(column.nunique(dropna=False)),
            "nullable": bool(column.isna().any()),
        }
    if hashing.business_key_column in df.columns:
        column = df[hashing.business_key_column]
        hashing_column_meta[hashing.business_key_column] = {
            "unique": int(column.nunique(dropna=False)),
            "nullable": bool(column.isna().any()),
        }
    if hashing_column_meta:
        base_metadata["hashing"].update(hashing_column_meta)

    if config.extends:
        base_metadata["config_extends"] = list(config.extends)
    row_column = hashing.row_hash_column
    business_column = hashing.business_key_column
    if row_column in df.columns and not df.empty:
        base_metadata["hashing"]["sample_hash_row"] = str(
            df.iloc[0][row_column]
        )
    if business_column in df.columns and not df.empty:
        base_metadata["hashing"]["sample_hash_business_key"] = str(
            df.iloc[0][business_column]
        )

    # Enrich hashing section with static policy details expected by golden meta
    base_metadata["hashing"].update(
        {
            "algorithm": hashing.algorithm,
            "business_key_column": hashing.business_key_column,
            "business_key_fields": list(hashing.business_key_fields),
            "row_column": hashing.row_hash_column,
            "row_fields": list(hashing.row_fields),
        }
    )
    return base_metadata


def build_write_artifacts(
    df: pd.DataFrame,
    *,
    config: PipelineConfig,
    run_id: str,
    pipeline_code: str,
    dataset_path: Path,
    stage_durations_ms: Mapping[str, float],
) -> DeterministicWriteArtifacts:
    """Prepare dataframe and metadata for persistent storage."""

    prepared = prepare_dataframe(df, config=config)
    prepared = ensure_hash_columns(prepared, config=config)
    metadata = serialise_metadata(
        prepared,
        config=config,
        run_id=run_id,
        pipeline_code=pipeline_code,
        dataset_path=dataset_path,
        stage_durations_ms=stage_durations_ms,
    )
    return DeterministicWriteArtifacts(dataframe=prepared, metadata=metadata)
