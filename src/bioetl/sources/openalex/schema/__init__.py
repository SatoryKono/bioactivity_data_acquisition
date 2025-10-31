"""Pandera schemas for OpenAlex raw and normalized datasets."""

from __future__ import annotations

import pandas as pd

from bioetl.pandera_pandas import pa
from bioetl.pandera_typing import Series
from bioetl.schemas.base import BaseSchema

__all__ = ["OpenAlexRawSchema", "OpenAlexNormalizedSchema"]


class OpenAlexRawSchema(pa.DataFrameModel):
    """Schema describing the OpenAlex ``/works`` payload prior to normalization."""

    id: Series[str] = pa.Field(nullable=False, description="Canonical OpenAlex work identifier")
    title: Series[str] = pa.Field(nullable=True, description="Title as returned by OpenAlex")
    doi: Series[str] = pa.Field(nullable=True, description="Raw DOI string from OpenAlex")
    authorships: Series[object] = pa.Field(nullable=True, description="Authorship metadata list")
    primary_location: Series[object] = pa.Field(nullable=True, description="Primary location dictionary")
    publication_date: Series[str] = pa.Field(nullable=True, description="Publication date (ISO8601)")
    publication_year: Series[pd.Int64Dtype] = pa.Field(nullable=True, description="Publication year from OpenAlex")
    type: Series[str] = pa.Field(nullable=True, description="Work type reported by OpenAlex")
    language: Series[str] = pa.Field(nullable=True, description="Language code reported by OpenAlex")
    open_access: Series[object] = pa.Field(nullable=True, description="Open access metadata block")
    concepts: Series[object] = pa.Field(nullable=True, description="List of concept dictionaries")
    ids: Series[object] = pa.Field(nullable=True, description="Cross-reference identifier mapping")
    abstract: Series[str] = pa.Field(nullable=True, description="Abstract text when available")

    class Config:
        strict = True
        coerce = True
        ordered = False


class OpenAlexNormalizedSchema(BaseSchema):
    """Schema for OpenAlex enrichment outputs ready for document merging."""

    doi_clean: Series[str] = pa.Field(nullable=True, description="Normalized DOI used for joins")
    openalex_doi: Series[str] = pa.Field(nullable=True, description="OpenAlex DOI after normalization")
    openalex_doi_clean: Series[str] = pa.Field(nullable=True, description="Duplicate-safe DOI key")
    openalex_id: Series[str] = pa.Field(nullable=True, description="Primary OpenAlex identifier")
    openalex_pmid: Series[pd.Int64Dtype] = pa.Field(nullable=True, description="PubMed ID derived from OpenAlex")
    openalex_title: Series[str] = pa.Field(nullable=True, description="Title normalized from OpenAlex")
    openalex_journal: Series[str] = pa.Field(nullable=True, description="Journal display name")
    openalex_authors: Series[str] = pa.Field(nullable=True, description="Author list (semicolon separated)")
    openalex_year: Series[pd.Int64Dtype] = pa.Field(nullable=True, description="Publication year")
    openalex_month: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=12, description="Publication month")
    openalex_day: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=31, description="Publication day")
    openalex_publication_date: Series[str] = pa.Field(nullable=True, description="Full publication date")
    openalex_type: Series[str] = pa.Field(nullable=True, description="Document type reported by OpenAlex")
    openalex_language: Series[str] = pa.Field(nullable=True, description="Language code reported by OpenAlex")
    openalex_is_oa: Series[pd.BooleanDtype] = pa.Field(nullable=True, description="Open access availability flag")
    openalex_oa_status: Series[str] = pa.Field(nullable=True, description="Open access status label")
    openalex_oa_url: Series[str] = pa.Field(nullable=True, description="Canonical OA URL")
    openalex_issn: Series[str] = pa.Field(nullable=True, description="ISSN or ISSN-L from venue")
    openalex_concepts_top3: Series[object] = pa.Field(nullable=True, description="Top 3 concept display names")
    openalex_landing_page: Series[str] = pa.Field(nullable=True, description="Landing page URL")
    openalex_doc_type: Series[str] = pa.Field(nullable=True, description="Document type mapped to document schema")
    openalex_crossref_doc_type: Series[str] = pa.Field(nullable=True, description="Crossref document type reported by OpenAlex")
    openalex_citation_count: Series[pd.Int64Dtype] = pa.Field(nullable=True, description="Citation count when supplied")

    class Config:
        strict = True
        coerce = True
        ordered = False

    _column_order = [
        "index",
        "hash_row",
        "hash_business_key",
        "pipeline_version",
        "run_id",
        "source_system",
        "chembl_release",
        "extracted_at",
        "doi_clean",
        "openalex_doi",
        "openalex_doi_clean",
        "openalex_id",
        "openalex_pmid",
        "openalex_title",
        "openalex_journal",
        "openalex_authors",
        "openalex_year",
        "openalex_month",
        "openalex_day",
        "openalex_publication_date",
        "openalex_type",
        "openalex_language",
        "openalex_is_oa",
        "openalex_oa_status",
        "openalex_oa_url",
        "openalex_issn",
        "openalex_concepts_top3",
        "openalex_landing_page",
        "openalex_doc_type",
        "openalex_crossref_doc_type",
        "openalex_citation_count",
    ]
