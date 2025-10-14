"""Pandera schema for pipeline input queries."""

from __future__ import annotations

import pandera as pa

INPUT_SCHEMA = pa.DataFrameSchema(
    {
        "document_chembl_id": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
        "doi": pa.Column(pa.String, nullable=True, required=False),
        "pmid": pa.Column(pa.String, nullable=True, required=False),
    },
    strict=False,
    name="InputQueries",
)
