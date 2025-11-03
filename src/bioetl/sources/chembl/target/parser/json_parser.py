from __future__ import annotations

import json
from typing import Any

import pandas as pd

from bioetl.core.logger import UnifiedLogger

__all__ = ["expand_json_column", "expand_protein_classifications"]

logger = UnifiedLogger.get(__name__)


def expand_json_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Expand a JSON encoded column into a flat DataFrame."""

    if column not in df.columns:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for row in df.itertuples(index=False):
        payload = getattr(row, column, None)
        if payload is None or payload is pd.NA:
            continue
        if isinstance(payload, str) and payload.strip() in {"", "[]"}:
            continue
        if isinstance(payload, (list, tuple)) and not payload:
            continue

        if isinstance(payload, str):
            try:
                parsed = json.loads(payload)
            except json.JSONDecodeError:
                logger.debug("expand_json_column_invalid", column=column, value=payload)
                continue
        else:
            parsed = payload

        if isinstance(parsed, dict):
            parsed = [parsed]

        if not isinstance(parsed, list):
            continue

        for item in parsed:
            if not isinstance(item, dict):
                continue
            record = item.copy()
            if "target_chembl_id" not in record:
                record["target_chembl_id"] = getattr(row, "target_chembl_id", None)
            records.append(record)

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records).convert_dtypes()


def expand_protein_classifications(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise protein classification hierarchy."""

    raw_df = expand_json_column(df, "protein_classifications")
    if raw_df.empty:
        return pd.DataFrame(columns=["target_chembl_id", "class_level", "class_name", "full_path"])

    records: list[dict[str, Any]] = []
    level_keys = ["l1", "l2", "l3", "l4", "l5"]
    for entry in raw_df.to_dict("records"):
        target_id = entry.get("target_chembl_id")
        path: list[str] = []
        for idx, key in enumerate(level_keys, start=1):
            class_name = entry.get(key)
            if class_name in {None, "", pd.NA}:
                continue
            class_name_str = str(class_name)
            path.append(class_name_str)
            records.append(
                {
                    "target_chembl_id": target_id,
                    "class_level": f"L{idx}",
                    "class_name": class_name_str,
                    "full_path": " > ".join(path),
                }
            )

    if not records:
        return pd.DataFrame(columns=["target_chembl_id", "class_level", "class_name", "full_path"])

    protein_class_df = pd.DataFrame(records).drop_duplicates()
    return protein_class_df.convert_dtypes()
