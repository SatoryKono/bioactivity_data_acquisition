from __future__ import annotations

import pandera as pa

_COLUMNS = [
    "document_id",
    "doi",
    "title",
    "journal",
    "business_key",
    "business_key_hash",
    "row_hash",
]

DocumentSchema = pa.DataFrameSchema(
    {
        "document_id": pa.Column(pa.String, nullable=False),
        "doi": pa.Column(pa.String, nullable=True),
        "title": pa.Column(pa.String, nullable=True),
        "journal": pa.Column(pa.String, nullable=True),
        "business_key": pa.Column(pa.String, nullable=False),
        "business_key_hash": pa.Column(pa.String, nullable=False),
        "row_hash": pa.Column(pa.String, nullable=False),
    },
    strict=True,
    ordered=True,
    coerce=True,
    name="DocumentSchema",
)

DocumentColumns = tuple(_COLUMNS)

__all__ = ["DocumentSchema", "DocumentColumns"]
