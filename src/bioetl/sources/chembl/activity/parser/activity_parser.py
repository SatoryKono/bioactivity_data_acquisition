from __future__ import annotations

from typing import Any, Iterable, Mapping

import pandas as pd


def _extract_items(payload: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        results = payload.get("results")
        if isinstance(results, list):
            for item in results:
                if isinstance(item, Mapping):
                    yield item
        elif payload:
            yield payload
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, Mapping):
                yield item


def parse_activity_payload(payload: Any) -> pd.DataFrame:
    records = []
    for item in _extract_items(payload):
        records.append(
            {
                "activity_id": item.get("activity_id") or item.get("activity_chembl_id"),
                "assay_id": item.get("assay_id") or item.get("assay_chembl_id"),
                "target_id": item.get("target_chembl_id"),
                "value": item.get("standard_value") or item.get("value"),
                "unit": item.get("standard_units") or item.get("units"),
            }
        )
    return pd.DataFrame.from_records(records, columns=["activity_id", "assay_id", "target_id", "value", "unit"])
