from __future__ import annotations

from typing import Any

import pandas as pd

from bioetl.sources.chembl.common import ColumnMapping, build_records_from_payload


_TESTITEM_MAPPINGS = [
    ColumnMapping("test_item_id", ("molecule_chembl_id", "test_item_id")),
    ColumnMapping("name", ("pref_name", "name")),
    ColumnMapping("molecule_type", ("molecule_type",)),
    ColumnMapping("inchi_key", ("inchi_key",)),
]


def parse_testitem_payload(payload: Any) -> pd.DataFrame:
    records = build_records_from_payload(payload, _TESTITEM_MAPPINGS)
    columns = [mapping.column for mapping in _TESTITEM_MAPPINGS]
    return pd.DataFrame.from_records(records, columns=columns)
