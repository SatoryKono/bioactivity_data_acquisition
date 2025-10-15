"""Transformation logic for the ETL pipeline."""

from __future__ import annotations

import pandas as pd
from structlog.stdlib import BoundLogger

from ..validation import NormalizedBioactivitySchema, RawBioactivitySchema

_EMPTY_NORMALIZED_DTYPES: dict[str, str] = {
    "compound_id": "object",
    "target": "object",
    "activity_value": "float64",
    "activity_unit": "object",
    "source": "object",
    "retrieved_at": "datetime64[ns]",
    "smiles": "object",
}


def create_empty_normalized_frame() -> pd.DataFrame:
    """Return an empty, schema-compliant normalized bioactivity frame."""

    empty = pd.DataFrame({
        column: pd.Series(dtype=dtype) for column, dtype in _EMPTY_NORMALIZED_DTYPES.items()
    })
    return NormalizedBioactivitySchema.validate(empty, lazy=True)

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
        return create_empty_normalized_frame()

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


__all__ = ["normalize_bioactivity_data", "create_empty_normalized_frame"]
