"""Transformation logic for the ETL pipeline."""

from __future__ import annotations

import pandas as pd
from structlog.stdlib import BoundLogger

from bioactivity.schemas import NormalizedBioactivitySchema, RawBioactivitySchema

_UNIT_CONVERSION = {
    "nM": 1.0,
    "uM": 1000.0,
    "pM": 0.001,
}


def _convert_to_nanomolar(df: pd.DataFrame) -> pd.Series:
    factors = df["activity_units"].map(_UNIT_CONVERSION)
    if factors.isnull().any():
        unknown = sorted(df.loc[factors.isnull(), "activity_units"].unique())
        raise ValueError(f"Unsupported activity units: {', '.join(unknown)}")
    return df["activity_value"].astype(float) * factors


def normalize_bioactivity_data(df: pd.DataFrame, logger: BoundLogger | None = None) -> pd.DataFrame:
    """Normalize raw bioactivity data to a consistent schema."""

    raw_schema = RawBioactivitySchema.to_schema()
    validated = raw_schema.validate(df, lazy=True)
    if validated.empty:
        normalized_schema = NormalizedBioactivitySchema.to_schema()
        empty = normalized_schema.empty_dataframe()  # type: ignore[attr-defined]
        return normalized_schema.validate(empty, lazy=True)

    normalized = validated.copy()
    normalized["retrieved_at"] = pd.to_datetime(normalized["retrieved_at"], utc=True)
    normalized["activity_value"] = _convert_to_nanomolar(normalized)
    normalized["activity_unit"] = "nM"
    normalized = normalized.rename(columns={"target_pref_name": "target"})
    normalized = normalized.drop(columns=["activity_units"], errors="ignore")
    normalized = normalized[[
        "compound_id",
        "target",
        "activity_value",
        "activity_unit",
        "source",
        "retrieved_at",
        "smiles",
    ]]
    normalized = normalized.sort_values(["compound_id", "target"]).reset_index(drop=True)
    result = NormalizedBioactivitySchema.validate(normalized, lazy=True)
    if logger is not None:
        logger.info("transform_complete", rows=len(result))
    return result


__all__ = ["normalize_bioactivity_data"]
