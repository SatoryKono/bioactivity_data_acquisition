from __future__ import annotations

import pandera as pa

SCHEMA_VERSION = "1.0.0"
COLUMN_ORDER = ("id", "value", "hash_row", "load_meta_id")

SimpleSchema = pa.DataFrameSchema(
    {
        "id": pa.Column(pa.Int64, nullable=False),
        "value": pa.Column(pa.Float64, nullable=False),
        "hash_row": pa.Column(pa.String, nullable=True),
        "load_meta_id": pa.Column(pa.String, nullable=True),
    },
    strict=True,
    coerce=True,
    name="SimpleSchema",
    metadata={"column_order": list(COLUMN_ORDER)},
)
