from __future__ import annotations

from typing import Any

import pandas as pd

from bioetl.sources.chembl.common import ColumnMapping, build_records_from_payload


_ACTIVITY_MAPPINGS = [
    ColumnMapping("activity_id", ("activity_id", "activity_chembl_id")),
    ColumnMapping("assay_id", ("assay_id", "assay_chembl_id")),
    ColumnMapping("target_id", ("target_chembl_id",)),
    ColumnMapping("value", ("standard_value", "value")),
    ColumnMapping("unit", ("standard_units", "units")),
]


def parse_activity_payload(payload: Any) -> pd.DataFrame:
    records = build_records_from_payload(payload, _ACTIVITY_MAPPINGS)
    columns = [mapping.column for mapping in _ACTIVITY_MAPPINGS]
    return pd.DataFrame.from_records(records, columns=columns)
