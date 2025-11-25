from __future__ import annotations

import pandas as pd

from bioetl.core.output import hash_business_key, hash_row
from bioetl.schemas.document_schema import DocumentColumns, DocumentSchema


def normalize_document(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    df["document_id"] = df["document_id"].astype(str)
    df["doi"] = df.get("doi", pd.Series([], dtype="string")).astype("string")
    df["title"] = df.get("title", pd.Series([], dtype="string")).astype("string")
    df["journal"] = df.get("journal", pd.Series([], dtype="string")).astype("string")

    df["business_key"] = df["document_id"].astype(str)
    df["business_key_hash"] = df["business_key"].apply(hash_business_key)
    df["row_hash"] = df.apply(
        lambda row: hash_row([row[col] for col in DocumentColumns if col != "row_hash"]),
        axis=1,
    )

    validated = DocumentSchema.validate(df[list(DocumentColumns)])
    return validated
