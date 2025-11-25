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


def parse_testitem_payload(payload: Any) -> pd.DataFrame:
    records = []
    for item in _extract_items(payload):
        records.append(
            {
                "test_item_id": item.get("molecule_chembl_id") or item.get("test_item_id"),
                "name": item.get("pref_name") or item.get("name"),
                "molecule_type": item.get("molecule_type"),
                "inchi_key": item.get("inchi_key"),
            }
        )
    return pd.DataFrame.from_records(
        records,
        columns=["test_item_id", "name", "molecule_type", "inchi_key"],
    )
