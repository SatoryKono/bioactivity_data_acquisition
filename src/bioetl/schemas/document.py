"""Document pipeline schemas: input, raw, and normalized."""

from __future__ import annotations

from typing import Final

import pandas as pd
import pandera as pa
from pandera.typing import Series

from bioetl.schemas.base import BaseSchema

DOCUMENT_NORMALIZED_COLUMN_ORDER: Final[list[str]] = [
    "index",
    "hash_row",
    "hash_business_key",
    "pipeline_version",
    "source_system",
    "chembl_release",
    "extracted_at",
    "document_chembl_id",
    "title",
    "abstract",
    "journal",
    "year",
    "doi",
    "doi_clean",
    "pubmed_id",
    "authors",
    "authors_count",
    "conflict_doi",
    "conflict_pmid",
    "qc_flag_title_fallback_used",
]


class DocumentInputSchema(pa.DataFrameModel):
    """Schema for the list of document identifiers used as input."""

    document_chembl_id: Series[str] = pa.Field(
        nullable=False,
        unique=True,
        description="ChEMBL document identifier",
    )

    class Config:
        strict = True
        ordered = True
        coerce = True

    @pa.check("document_chembl_id")
    @classmethod
    def _validate_document_ids(cls, series: Series[str]) -> Series[bool]:
        """Ensure document identifiers follow the CHEMBL pattern."""

        return series.fillna("").str.match(r"^CHEMBL\d+$")


class DocumentRawSchema(pa.DataFrameModel):
    """Schema for raw documents after extraction and enrichment."""

    document_chembl_id: Series[str] = pa.Field(nullable=False)
    chembl_title: Series[str] = pa.Field(nullable=True)
    chembl_abstract: Series[str] = pa.Field(nullable=True)
    chembl_doi: Series[str] = pa.Field(nullable=True)
    chembl_pmid: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    chembl_journal: Series[str] = pa.Field(nullable=True)
    chembl_year: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1500, le=2100)
    chembl_authors: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = False  # allow additional enrichment columns
        ordered = False
        coerce = True


class DocumentNormalizedSchema(BaseSchema):
    """Normalized schema exported by the document pipeline."""

    document_chembl_id: Series[str] = pa.Field(
        regex=r"^CHEMBL\d+$",
        nullable=False,
        unique=True,
    )
    title: Series[str] = pa.Field(nullable=True, description="Final title after precedence merge")
    abstract: Series[str] = pa.Field(nullable=True)
    journal: Series[str] = pa.Field(nullable=True)
    year: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1500, le=2100)
    doi: Series[str] = pa.Field(nullable=True)
    doi_clean: Series[str] = pa.Field(nullable=True)
    pubmed_id: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    authors: Series[str] = pa.Field(nullable=True)
    authors_count: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    conflict_doi: Series[pd.BooleanDtype] = pa.Field(nullable=True)
    conflict_pmid: Series[pd.BooleanDtype] = pa.Field(nullable=True)
    qc_flag_title_fallback_used: Series[pd.Int64Dtype] = pa.Field(nullable=True, isin=[0, 1])

    class Config:
        strict = True
        ordered = True
        coerce = True


class DocumentSchema(DocumentNormalizedSchema):
    """Backward compatible alias for the normalized schema."""


__all__ = [
    "DocumentInputSchema",
    "DocumentRawSchema",
    "DocumentNormalizedSchema",
    "DocumentSchema",
    "DOCUMENT_NORMALIZED_COLUMN_ORDER",
]
