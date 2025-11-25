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


def parse_target_payload(payload: Any) -> pd.DataFrame:
    records = []
    for item in _extract_items(payload):
        records.append(
            {
                "target_id": item.get("target_chembl_id") or item.get("target_id"),
                "pref_name": item.get("pref_name"),
                "organism": item.get("organism"),
                "target_type": item.get("target_type"),
            }
        )
    return pd.DataFrame.from_records(records, columns=["target_id", "pref_name", "organism", "target_type"])
