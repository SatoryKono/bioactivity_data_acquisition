"""Pandera schema describing the expected input dataset."""
from __future__ import annotations

import pandera as pa
from pandera import Column, DataFrameSchema


def input_schema() -> DataFrameSchema:
    return DataFrameSchema(
        {
            "document_chembl_id": Column(pa.String, nullable=False),
            "doi": Column(pa.String, nullable=True, required=False),
            "pmid": Column(pa.String, nullable=True, required=False),
        },
        coerce=True,
        strict=False,
        name="input_publications",
    )


def validate_input(df):
    return input_schema().validate(df, lazy=True)

