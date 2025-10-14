"""Pandera schema describing the expected output dataset."""
from __future__ import annotations

import pandera as pa
from pandera import Column, DataFrameSchema


OUTPUT_COLUMNS = [
    "chembl.document_chembl_id",
    "doi_key",
    "pmid",
    "chembl.title",
    "chembl.abstract",
    "chembl.doi",
    "chembl.pmid",
    "crossref.doi",
    "crossref.title",
    "crossref.journal",
    "crossref.pmid",
    "openalex.doi",
    "openalex.title",
    "openalex.pmid",
    "pubmed.pmid",
    "pubmed.title",
    "pubmed.doi",
    "semscholar.paper_id",
    "semscholar.title",
    "semscholar.doi",
    "semscholar.pmid",
]


def output_schema() -> DataFrameSchema:
    return DataFrameSchema(
        {column: Column(pa.String, nullable=True, required=False) for column in OUTPUT_COLUMNS},
        coerce=True,
        strict=False,
        name="output_publications",
    )


def validate_output(df):
    return output_schema().validate(df, lazy=True)

