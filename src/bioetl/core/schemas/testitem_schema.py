from __future__ import annotations

import pandera as pa

_COLUMNS = [
    "test_item_id",
    "name",
    "molecule_type",
    "inchi_key",
    "business_key",
    "business_key_hash",
    "row_hash",
]

TestItemSchema = pa.DataFrameSchema(
    {
        "test_item_id": pa.Column(pa.String, nullable=False),
        "name": pa.Column(pa.String, nullable=True),
        "molecule_type": pa.Column(pa.String, nullable=True),
        "inchi_key": pa.Column(pa.String, nullable=True),
        "business_key": pa.Column(pa.String, nullable=False),
        "business_key_hash": pa.Column(pa.String, nullable=False),
        "row_hash": pa.Column(pa.String, nullable=False),
    },
    strict=True,
    ordered=True,
    coerce=True,
    name="TestItemSchema",
)

TestItemColumns = tuple(_COLUMNS)

__all__ = ["TestItemSchema", "TestItemColumns"]
