from __future__ import annotations

import pandas as pd

from bioetl.core.output import hash_business_key, hash_row
from bioetl.schemas.testitem_schema import TestItemColumns, TestItemSchema


def normalize_testitem(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    df["test_item_id"] = df["test_item_id"].astype(str)
    df["name"] = df.get("name", pd.Series([], dtype="string")).astype("string")
    df["molecule_type"] = df.get("molecule_type", pd.Series([], dtype="string")).astype("string")
    df["inchi_key"] = df.get("inchi_key", pd.Series([], dtype="string")).astype("string")

    df["business_key"] = df["test_item_id"].astype(str)
    df["business_key_hash"] = df["business_key"].apply(hash_business_key)
    df["row_hash"] = df.apply(
        lambda row: hash_row([row[col] for col in TestItemColumns if col != "row_hash"]),
        axis=1,
    )

    validated = TestItemSchema.validate(df[list(TestItemColumns)])
    return validated
