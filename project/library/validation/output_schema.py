"""Pandera schema for pipeline output."""
from __future__ import annotations

import pandera as pa
from pandera import Column, DataFrameSchema


class OutputColumns:
    DOCUMENT_ID = "chembl_document_chembl_id"
    DOI_KEY = "doi_key"
    PMID = "pmid"


output_schema = DataFrameSchema(
    {
        OutputColumns.DOCUMENT_ID: Column(pa.String, nullable=False),
        OutputColumns.DOI_KEY: Column(pa.String, nullable=True),
        OutputColumns.PMID: Column(pa.String, nullable=True),
    },
    coerce=True,
    strict=False,
)

OUTPUT_SCHEMA = output_schema