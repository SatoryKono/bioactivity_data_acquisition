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


def parse_document_payload(payload: Any) -> pd.DataFrame:
    records = []
    for item in _extract_items(payload):
        records.append(
            {
                "document_id": item.get("document_chembl_id") or item.get("document_id"),
                "doi": item.get("doi"),
                "title": item.get("title"),
                "journal": item.get("journal"),
            }
        )
    return pd.DataFrame.from_records(records, columns=["document_id", "doi", "title", "journal"])
