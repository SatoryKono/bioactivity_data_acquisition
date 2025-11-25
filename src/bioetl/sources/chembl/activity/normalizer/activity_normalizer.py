from __future__ import annotations

import pandas as pd

from bioetl.core.output import hash_business_key, hash_row
from bioetl.schemas.activity_schema import ActivityColumns, ActivitySchema


def normalize_activity(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    df["activity_id"] = df["activity_id"].astype(str)
    df["assay_id"] = df["assay_id"].astype(str)
    if "target_id" not in df:
        df["target_id"] = pd.NA
    df["target_id"] = df["target_id"].astype("string")
    df["value"] = pd.to_numeric(df.get("value"), errors="coerce")
    df["unit"] = df.get("unit", pd.Series([], dtype="string")).astype("string")

    df["business_key"] = df["activity_id"].astype(str)
    df["business_key_hash"] = df["business_key"].apply(hash_business_key)
    df["row_hash"] = df.apply(
        lambda row: hash_row([row[col] for col in ActivityColumns if col != "row_hash"]),
        axis=1,
    )

    validated = ActivitySchema.validate(df[list(ActivityColumns)])
    return validated
