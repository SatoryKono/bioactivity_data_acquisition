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


def parse_assay_payload(payload: Any) -> pd.DataFrame:
    records = []
    for item in _extract_items(payload):
        records.append(
            {
                "assay_id": item.get("assay_chembl_id") or item.get("assay_id"),
                "assay_type": item.get("assay_type"),
                "description": item.get("description"),
                "target_id": item.get("target_chembl_id"),
            }
        )
    return pd.DataFrame.from_records(records, columns=["assay_id", "assay_type", "description", "target_id"])
