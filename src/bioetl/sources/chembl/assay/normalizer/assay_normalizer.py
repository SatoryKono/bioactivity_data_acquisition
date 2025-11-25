from __future__ import annotations

import pandas as pd

from bioetl.core.output import hash_business_key, hash_row
from bioetl.schemas.assay_schema import AssayColumns, AssaySchema


def normalize_assay(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    df["assay_id"] = df["assay_id"].astype(str)
    df["assay_type"] = df.get("assay_type", pd.Series([], dtype="string")).astype("string")
    df["description"] = df.get("description", pd.Series([], dtype="string")).astype("string")
    if "target_id" not in df:
        df["target_id"] = pd.NA
    df["target_id"] = df["target_id"].astype("string")

    df["business_key"] = df["assay_id"].astype(str)
    df["business_key_hash"] = df["business_key"].apply(hash_business_key)
    df["row_hash"] = df.apply(
        lambda row: hash_row([row[col] for col in AssayColumns if col != "row_hash"]),
        axis=1,
    )

    validated = AssaySchema.validate(df[list(AssayColumns)])
    return validated
