"""Pandera schema for pipeline input queries."""

from __future__ import annotations

import pandera as pa

INPUT_SCHEMA = pa.DataFrameSchema(
    {
        "query": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
        "type": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
    },
    strict=True,
    name="InputQueries",
)
