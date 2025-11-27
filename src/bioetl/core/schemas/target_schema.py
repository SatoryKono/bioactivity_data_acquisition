from __future__ import annotations

import pandera as pa

_COLUMNS = [
    "target_id",
    "pref_name",
    "organism",
    "target_type",
    "business_key",
    "business_key_hash",
    "row_hash",
]

TargetSchema = pa.DataFrameSchema(
    {
        "target_id": pa.Column(pa.String, nullable=False),
        "pref_name": pa.Column(pa.String, nullable=True),
        "organism": pa.Column(pa.String, nullable=True),
        "target_type": pa.Column(pa.String, nullable=True),
        "business_key": pa.Column(pa.String, nullable=False),
        "business_key_hash": pa.Column(pa.String, nullable=False),
        "row_hash": pa.Column(pa.String, nullable=False),
    },
    strict=True,
    ordered=True,
    coerce=True,
    name="TargetSchema",
)

TargetColumns = tuple(_COLUMNS)

__all__ = ["TargetSchema", "TargetColumns"]
