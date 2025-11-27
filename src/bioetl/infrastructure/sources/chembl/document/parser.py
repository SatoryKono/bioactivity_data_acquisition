from __future__ import annotations

from typing import Any

import pandas as pd

from bioetl.sources.chembl.common import ColumnMapping, build_records_from_payload


_DOCUMENT_MAPPINGS = [
    ColumnMapping("document_id", ("document_chembl_id", "document_id")),
    ColumnMapping("doi", ("doi",)),
    ColumnMapping("title", ("title",)),
    ColumnMapping("journal", ("journal",)),
]


def parse_document_payload(payload: Any) -> pd.DataFrame:
    records = build_records_from_payload(payload, _DOCUMENT_MAPPINGS)
    columns = [mapping.column for mapping in _DOCUMENT_MAPPINGS]
    return pd.DataFrame.from_records(records, columns=columns)
