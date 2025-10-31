"""Pandera schema definitions for Semantic Scholar payloads."""

from __future__ import annotations

import pandas as pd

from bioetl.pandera_pandas import DataFrameModel, pa
from bioetl.pandera_typing import Series

__all__ = [
    "SemanticScholarRawSchema",
    "SemanticScholarNormalizedSchema",
]


class SemanticScholarRawSchema(DataFrameModel):
    """Schema for validating raw Semantic Scholar API responses."""

    paperId: Series[str] = pa.Field(nullable=False)
    title: Series[str] = pa.Field(nullable=True)
    abstract: Series[str] = pa.Field(nullable=True)
    venue: Series[str] = pa.Field(nullable=True)
    year: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    publicationDate: Series[str] = pa.Field(nullable=True)
    externalIds: Series[object] = pa.Field(nullable=True)
    authors: Series[object] = pa.Field(nullable=True)
    citationCount: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    influentialCitationCount: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    referenceCount: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    isOpenAccess: Series[pd.BooleanDtype] = pa.Field(nullable=True)
    publicationTypes: Series[object] = pa.Field(nullable=True)
    fieldsOfStudy: Series[object] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
        ordered = False


class SemanticScholarNormalizedSchema(DataFrameModel):
    """Schema describing normalized Semantic Scholar enrichment records."""

    doi_clean: Series[str] = pa.Field(nullable=True)
    paper_id: Series[str] = pa.Field(nullable=True)
    pubmed_id: Series[str] = pa.Field(nullable=True)
    title: Series[str] = pa.Field(nullable=True)
    journal: Series[str] = pa.Field(nullable=True)
    authors: Series[str] = pa.Field(nullable=True)
    abstract: Series[str] = pa.Field(nullable=True)
    year: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    publication_date: Series[str] = pa.Field(nullable=True)
    citation_count: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    influential_citations: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    reference_count: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    is_oa: Series[pd.BooleanDtype] = pa.Field(nullable=True)
    publication_types: Series[object] = pa.Field(nullable=True)
    doc_type: Series[str] = pa.Field(nullable=True)
    fields_of_study: Series[object] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
        ordered = False

    _column_order = [
        "doi_clean",
        "paper_id",
        "pubmed_id",
        "title",
        "journal",
        "authors",
        "abstract",
        "year",
        "publication_date",
        "citation_count",
        "influential_citations",
        "reference_count",
        "is_oa",
        "publication_types",
        "doc_type",
        "fields_of_study",
    ]
