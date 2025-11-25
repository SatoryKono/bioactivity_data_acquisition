from __future__ import annotations

import pandera as pa

_COLUMNS = [
    "assay_id",
    "assay_type",
    "description",
    "target_id",
    "business_key",
    "business_key_hash",
    "row_hash",
]

AssaySchema = pa.DataFrameSchema(
    {
        "assay_id": pa.Column(pa.String, nullable=False),
        "assay_type": pa.Column(pa.String, nullable=True),
        "description": pa.Column(pa.String, nullable=True),
        "target_id": pa.Column(pa.String, nullable=True),
        "business_key": pa.Column(pa.String, nullable=False),
        "business_key_hash": pa.Column(pa.String, nullable=False),
        "row_hash": pa.Column(pa.String, nullable=False),
    },
    strict=True,
    ordered=True,
    coerce=True,
    name="AssaySchema",
)

AssayColumns = tuple(_COLUMNS)

__all__ = ["AssaySchema", "AssayColumns"]
