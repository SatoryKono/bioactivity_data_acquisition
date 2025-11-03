"""Pandera schemas describing PubMed ingestion stages."""

from __future__ import annotations

import pandas as pd

from bioetl.pandera_pandas import pa
from bioetl.pandera_typing import Series
from bioetl.schemas.base import BaseSchema

__all__ = ["PubMedRawSchema", "PubMedNormalizedSchema"]


class PubMedRawSchema(BaseSchema):
    """Schema representing parsed ``efetch`` payloads before normalization."""

    pmid: Series[pd.Int64Dtype] = pa.Field(nullable=False, ge=1)
    title: Series[str] = pa.Field(nullable=True)
    abstract: Series[str] = pa.Field(nullable=True)
    journal: Series[str] = pa.Field(nullable=True)
    journal_abbrev: Series[str] = pa.Field(nullable=True)
    volume: Series[str] = pa.Field(nullable=True)
    issue: Series[str] = pa.Field(nullable=True)
    first_page: Series[str] = pa.Field(nullable=True)
    last_page: Series[str] = pa.Field(nullable=True)
    year: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1500)
    month: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=12)
    day: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=31)
    issn: Series[object] = pa.Field(nullable=True)
    doi: Series[str] = pa.Field(nullable=True)
    authors: Series[object] = pa.Field(nullable=True)
    mesh_terms: Series[object] = pa.Field(nullable=True)
    chemicals: Series[object] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
        ordered = False


class PubMedNormalizedSchema(BaseSchema):
    """Schema representing normalized PubMed enrichment records."""

    pmid: Series[pd.Int64Dtype] = pa.Field(nullable=False, ge=1)
    pubmed_pmid: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1)
    doi_clean: Series[str] = pa.Field(nullable=True)
    pubmed_article_title: Series[str] = pa.Field(nullable=True)
    pubmed_abstract: Series[str] = pa.Field(nullable=True)
    pubmed_journal: Series[str] = pa.Field(nullable=True)
    pubmed_journal_abbrev: Series[str] = pa.Field(nullable=True)
    pubmed_year: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1500)
    pubmed_month: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=12)
    pubmed_day: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=31)
    pubmed_volume: Series[str] = pa.Field(nullable=True)
    pubmed_issue: Series[str] = pa.Field(nullable=True)
    pubmed_first_page: Series[str] = pa.Field(nullable=True)
    pubmed_last_page: Series[str] = pa.Field(nullable=True)
    pubmed_doi: Series[str] = pa.Field(nullable=True)
    pubmed_issn: Series[str] = pa.Field(nullable=True)
    pubmed_issn_print: Series[str] = pa.Field(nullable=True)
    pubmed_issn_electronic: Series[str] = pa.Field(nullable=True)
    pubmed_authors: Series[str] = pa.Field(nullable=True)
    pubmed_mesh_descriptors: Series[str] = pa.Field(nullable=True)
    pubmed_mesh_qualifiers: Series[str] = pa.Field(nullable=True)
    pubmed_chemical_list: Series[str] = pa.Field(nullable=True)
    pubmed_doc_type: Series[str] = pa.Field(nullable=True)
    pubmed_year_completed: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1500)
    pubmed_month_completed: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=12)
    pubmed_day_completed: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=31)
    pubmed_year_revised: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1500)
    pubmed_month_revised: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=12)
    pubmed_day_revised: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=31)

    class Config:
        strict = True
        coerce = True
        ordered = False

    _column_order = [
        "pmid",
        "pubmed_pmid",
        "doi_clean",
        "pubmed_article_title",
        "pubmed_abstract",
        "pubmed_journal",
        "pubmed_journal_abbrev",
        "pubmed_year",
        "pubmed_month",
        "pubmed_day",
        "pubmed_volume",
        "pubmed_issue",
        "pubmed_first_page",
        "pubmed_last_page",
        "pubmed_doi",
        "pubmed_issn",
        "pubmed_issn_print",
        "pubmed_issn_electronic",
        "pubmed_authors",
        "pubmed_mesh_descriptors",
        "pubmed_mesh_qualifiers",
        "pubmed_chemical_list",
        "pubmed_doc_type",
        "pubmed_year_completed",
        "pubmed_month_completed",
        "pubmed_day_completed",
        "pubmed_year_revised",
        "pubmed_month_revised",
        "pubmed_day_revised",
    ]
