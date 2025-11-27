from __future__ import annotations

"""Parsing helpers for ChEMBL activity payloads."""

from typing import Any

import pandas as pd

from bioetl.infrastructure.sources.chembl.common import (
    ColumnMapping,
    build_records_from_payload,
)


_ACTIVITY_MAPPINGS = [
    ColumnMapping("activity_id", ("activity_id", "activity_chembl_id")),
    ColumnMapping("assay_id", ("assay_id", "assay_chembl_id")),
    ColumnMapping("target_id", ("target_chembl_id",)),
    ColumnMapping("value", ("standard_value", "value")),
    ColumnMapping("unit", ("standard_units", "units")),
]


class ActivityParser:
    """Convert raw API responses into a normalized tabular form."""

    def parse(self, raw_json: Any) -> pd.DataFrame:
        """Parse raw API JSON into a dataframe with canonical columns."""

        records = build_records_from_payload(raw_json, _ACTIVITY_MAPPINGS)
        columns = [mapping.column for mapping in _ACTIVITY_MAPPINGS]
        return pd.DataFrame.from_records(records, columns=columns)


__all__ = ["ActivityParser"]
