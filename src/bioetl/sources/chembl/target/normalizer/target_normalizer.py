from __future__ import annotations

import pandas as pd

from bioetl.core.output import hash_business_key, hash_row
from bioetl.schemas.target_schema import TargetColumns, TargetSchema


def normalize_target(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    df["target_id"] = df["target_id"].astype(str)
    df["pref_name"] = df.get("pref_name", pd.Series([], dtype="string")).astype("string")
    df["organism"] = df.get("organism", pd.Series([], dtype="string")).astype("string")
    df["target_type"] = df.get("target_type", pd.Series([], dtype="string")).astype("string")

    df["business_key"] = df["target_id"].astype(str)
    df["business_key_hash"] = df["business_key"].apply(hash_business_key)
    df["row_hash"] = df.apply(
        lambda row: hash_row([row[col] for col in TargetColumns if col != "row_hash"]),
        axis=1,
    )

    validated = TargetSchema.validate(df[list(TargetColumns)])
    return validated
