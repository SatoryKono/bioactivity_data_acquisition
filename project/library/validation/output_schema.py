"""Pandera schema for normalized publication records."""

from __future__ import annotations

import pandera as pa

OUTPUT_COLUMNS = ["source", "identifier", "title", "published_at", "doi"]

OUTPUT_SCHEMA = pa.DataFrameSchema(
    {
        "source": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
        "identifier": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
        "title": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
        "published_at": pa.Column(pa.DateTime, nullable=True, coerce=True),
        "doi": pa.Column(pa.String, nullable=True),
    },
    strict=True,
    name="Publications",
)
