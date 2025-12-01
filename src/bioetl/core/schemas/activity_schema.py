from __future__ import annotations

import pandera.pandas as pa

_COLUMNS = [
    "activity_id",
    "assay_id",
    "target_id",
    "value",
    "unit",
    "business_key",
    "business_key_hash",
    "row_hash",
]

ActivitySchema = pa.DataFrameSchema(
    {
        "activity_id": pa.Column(pa.String, nullable=False),
        "assay_id": pa.Column(pa.String, nullable=False),
        "target_id": pa.Column(pa.String, nullable=True),
        "value": pa.Column(pa.Float, nullable=True),
        "unit": pa.Column(pa.String, nullable=True),
        "business_key": pa.Column(pa.String, nullable=False),
        "business_key_hash": pa.Column(pa.String, nullable=False),
        "row_hash": pa.Column(pa.String, nullable=False),
    },
    strict=True,
    ordered=True,
    coerce=True,
    name="ActivitySchema",
)

ActivityColumns = tuple(_COLUMNS)

__all__ = ["ActivitySchema", "ActivityColumns"]
