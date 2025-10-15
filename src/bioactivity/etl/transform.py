"""Transformation logic for the ETL pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from structlog.stdlib import BoundLogger

from bioactivity.schemas import NormalizedBioactivitySchema, RawBioactivitySchema

if TYPE_CHECKING:  # pragma: no cover - for type checking only
    from bioactivity.config import DeterminismSettings, TransformSettings


def _resolve_ascending(by: list[str], ascending: list[bool] | bool) -> list[bool] | bool:
    """Normalize the ascending parameter for ``DataFrame.sort_values``."""

    if isinstance(ascending, bool):
        return ascending
    if len(ascending) == len(by):
        return ascending
    if len(ascending) == 1:
        return ascending * len(by)
    raise ValueError("Ascending configuration must be a bool or match the sort keys length.")


def _convert_to_nanomolar(df: pd.DataFrame, unit_conversion: dict[str, float]) -> pd.Series:
    factors = df["activity_units"].map(unit_conversion)
    if factors.isnull().any():
        unknown = sorted(df.loc[factors.isnull(), "activity_units"].unique())
        raise ValueError(f"Unsupported activity units: {', '.join(unknown)}")
    return df["activity_value"].astype(float) * factors


def normalize_bioactivity_data(
    df: pd.DataFrame,
    *,
    transforms: TransformSettings | None = None,
    determinism: DeterminismSettings | None = None,
    logger: BoundLogger | None = None,
) -> pd.DataFrame:
    """Normalize raw bioactivity data to a consistent schema using configuration."""

    from bioactivity.config import DeterminismSettings as _DeterminismSettings
    from bioactivity.config import TransformSettings as _TransformSettings

    transforms = transforms or _TransformSettings()
    determinism = determinism or _DeterminismSettings()

    raw_schema = RawBioactivitySchema.to_schema()
    validated = raw_schema.validate(df, lazy=True)
    if validated.empty:
        normalized_schema = NormalizedBioactivitySchema.to_schema()
        empty = normalized_schema.empty_dataframe()  # type: ignore[attr-defined]
        return normalized_schema.validate(empty, lazy=True)

    normalized = validated.copy()
    normalized["retrieved_at"] = pd.to_datetime(normalized["retrieved_at"], utc=True)
    normalized["activity_value"] = _convert_to_nanomolar(normalized, transforms.unit_conversion)
    normalized["activity_unit"] = "nM"
    normalized = normalized.rename(columns={"target_pref_name": "target"})
    normalized = normalized.drop(columns=["activity_units"], errors="ignore")

    desired_order = [col for col in determinism.column_order if col in normalized.columns]
    remaining = [col for col in normalized.columns if col not in desired_order]
    ordered_columns = desired_order + remaining
    normalized = normalized[ordered_columns]

    sort_by = determinism.sort.by or ordered_columns
    sort_ascending = _resolve_ascending(sort_by, determinism.sort.ascending)
    normalized = normalized.sort_values(
        sort_by,
        ascending=sort_ascending,
        na_position=determinism.sort.na_position,
    ).reset_index(drop=True)

    result = NormalizedBioactivitySchema.validate(normalized, lazy=True)
    if logger is not None:
        logger.info("transform_complete", rows=len(result))
    return result


__all__ = ["normalize_bioactivity_data"]
