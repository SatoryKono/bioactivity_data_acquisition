from __future__ import annotations

"""Normalization logic for ChEMBL activity records."""

import pandas as pd

from bioetl.core.io.artifacts import hash_business_key, hash_row
from bioetl.schemas.activity_schema import ActivityColumns, ActivitySchema


class ActivityNormalizer:
    """Apply domain normalization and schema alignment for activity rows."""

    def normalize(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        df = df_raw.copy()
        df["activity_id"] = df.get("activity_id").astype("string")
        df["assay_id"] = df.get("assay_id").astype("string")
        if "target_id" not in df:
            df["target_id"] = pd.NA
        df["target_id"] = df.get("target_id").astype("string")
        df["value"] = pd.to_numeric(df.get("value"), errors="coerce")
        df["unit"] = df.get("unit", pd.Series([], dtype="string")).astype("string")

        df["business_key"] = df["activity_id"].astype(str)
        df["business_key_hash"] = df["business_key"].apply(hash_business_key)
        df["row_hash"] = df.apply(
            lambda row: hash_row([row[col] for col in ActivityColumns if col != "row_hash"]), axis=1
        )

        validated = ActivitySchema.validate(df[list(ActivityColumns)])
        return validated


__all__ = ["ActivityNormalizer"]
