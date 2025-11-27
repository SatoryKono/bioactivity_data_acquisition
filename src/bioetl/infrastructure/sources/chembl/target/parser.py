from __future__ import annotations

from typing import Any

import pandas as pd

from bioetl.infrastructure.sources.chembl.common import (
    ColumnMapping,
    build_records_from_payload,
)


_TARGET_MAPPINGS = [
    ColumnMapping("target_id", ("target_chembl_id", "target_id")),
    ColumnMapping("pref_name", ("pref_name",)),
    ColumnMapping("organism", ("organism",)),
    ColumnMapping("target_type", ("target_type",)),
]


def parse_target_payload(payload: Any) -> pd.DataFrame:
    records = build_records_from_payload(payload, _TARGET_MAPPINGS)
    columns = [mapping.column for mapping in _TARGET_MAPPINGS]
    return pd.DataFrame.from_records(records, columns=columns)
