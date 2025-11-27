from __future__ import annotations

from typing import Any

import pandas as pd

from bioetl.infrastructure.sources.chembl.common import (
    ColumnMapping,
    build_records_from_payload,
)


_ASSAY_MAPPINGS = [
    ColumnMapping("assay_id", ("assay_chembl_id", "assay_id")),
    ColumnMapping("assay_type", ("assay_type",)),
    ColumnMapping("description", ("description",)),
    ColumnMapping("target_id", ("target_chembl_id",)),
]


def parse_assay_payload(payload: Any) -> pd.DataFrame:
    records = build_records_from_payload(payload, _ASSAY_MAPPINGS)
    columns = [mapping.column for mapping in _ASSAY_MAPPINGS]
    return pd.DataFrame.from_records(records, columns=columns)
