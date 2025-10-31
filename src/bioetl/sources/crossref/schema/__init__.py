"""Pandera schemas for Crossref extraction and normalization."""

from __future__ import annotations

import pandas as pd

from bioetl.pandera_pandas import pa
from bioetl.pandera_typing import Series
from bioetl.schemas.base import BaseSchema

__all__ = ["CrossrefRawSchema", "CrossrefNormalizedSchema"]


class CrossrefRawSchema(BaseSchema):
    """Schema describing Crossref API payloads captured during extraction."""

    DOI: Series[str] = pa.Field(nullable=False)
    title: Series[object] = pa.Field(nullable=True)
    author: Series[object] = pa.Field(nullable=True)
    publisher: Series[str] = pa.Field(nullable=True)
    subject: Series[object] = pa.Field(nullable=True)
    type: Series[str] = pa.Field(nullable=True)
    container_title: Series[object] = pa.Field(nullable=True)
    short_container_title: Series[object] = pa.Field(nullable=True)
    published_print: Series[object] = pa.Field(nullable=True)
    published_online: Series[object] = pa.Field(nullable=True)
    issued: Series[object] = pa.Field(nullable=True)
    created: Series[object] = pa.Field(nullable=True)
    volume: Series[object] = pa.Field(nullable=True)
    issue: Series[object] = pa.Field(nullable=True)
    page: Series[object] = pa.Field(nullable=True)
    ISSN: Series[object] = pa.Field(nullable=True)
    issn_type: Series[object] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
        ordered = False


class CrossrefNormalizedSchema(BaseSchema):
    """Schema describing normalized Crossref enrichment records."""

    doi_clean: Series[str] = pa.Field(nullable=False)
    crossref_doi: Series[str] = pa.Field(nullable=True)

    title: Series[str] = pa.Field(nullable=True)
    journal: Series[str] = pa.Field(nullable=True)
    authors: Series[str] = pa.Field(nullable=True)

    year: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1800, le=2100)
    month: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=12)
    day: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=31)

    volume: Series[str] = pa.Field(nullable=True)
    issue: Series[str] = pa.Field(nullable=True)
    first_page: Series[str] = pa.Field(nullable=True)

    issn_print: Series[str] = pa.Field(nullable=True)
    issn_electronic: Series[str] = pa.Field(nullable=True)
    orcid: Series[str] = pa.Field(nullable=True)

    publisher: Series[str] = pa.Field(nullable=True)
    subject: Series[str] = pa.Field(nullable=True)
    doc_type: Series[str] = pa.Field(nullable=True)

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
        "crossref_doi",
        "title",
        "journal",
        "authors",
        "year",
        "month",
        "day",
        "volume",
        "issue",
        "first_page",
        "issn_print",
        "issn_electronic",
        "orcid",
        "publisher",
        "subject",
        "doc_type",
    ]
