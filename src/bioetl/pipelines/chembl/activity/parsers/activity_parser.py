from __future__ import annotations

"""Parsing helpers for ChEMBL activity payloads."""

from typing import Any, Iterable, Mapping

import pandas as pd


class ActivityParser:
    """Convert raw API responses into a normalized tabular form."""

    def _extract_items(self, payload: Any) -> Iterable[Mapping[str, Any]]:
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

    def parse(self, raw_json: Any) -> pd.DataFrame:
        """Parse raw API JSON into a dataframe with canonical columns."""

        records = []
        for item in self._extract_items(raw_json):
            records.append(
                {
                    "activity_id": item.get("activity_id") or item.get("activity_chembl_id"),
                    "assay_id": item.get("assay_id") or item.get("assay_chembl_id"),
                    "target_id": item.get("target_chembl_id"),
                    "value": item.get("standard_value") or item.get("value"),
                    "unit": item.get("standard_units") or item.get("units"),
                }
            )
        return pd.DataFrame.from_records(
            records, columns=["activity_id", "assay_id", "target_id", "value", "unit"]
        )


__all__ = ["ActivityParser"]
