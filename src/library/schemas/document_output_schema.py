"""Pandera schemas for document output data."""

from __future__ import annotations

import importlib.util

_PANDERA_PANDAS_SPEC = importlib.util.find_spec("pandera.pandas")
if _PANDERA_PANDAS_SPEC is not None:  # pragma: no cover - import side effect
    import pandera.pandas as pa  # type: ignore[no-redef]
else:  # pragma: no cover - import side effect
    import pandera as pa
from pandera.typing import Series  # noqa: E402


class DocumentOutputSchema(pa.DataFrameModel):
    """Schema for enriched document data from multiple sources."""

    # Original ChEMBL fields
    document_chembl_id: Series[str] = pa.Field(description="ChEMBL document identifier")
    title: Series[str] = pa.Field(description="Document title")
    doi: Series[str] = pa.Field(nullable=True, description="Digital Object Identifier")
    pubmed_id: Series[str] = pa.Field(nullable=True, description="PubMed identifier")
    doc_type: Series[str] = pa.Field(nullable=True, description="Document type")
    journal: Series[str] = pa.Field(nullable=True, description="Journal name")
    year: Series[int] = pa.Field(nullable=True, description="Publication year")
    
    # Legacy ChEMBL fields (keeping for backward compatibility)
    abstract: Series[str] = pa.Field(nullable=True, description="Document abstract")
    authors: Series[str] = pa.Field(nullable=True, description="Document authors")
    classification: Series[float] = pa.Field(nullable=True, description="Document classification")
    document_contains_external_links: Series[bool] = pa.Field(
        nullable=True, description="Contains external links"
    )
    first_page: Series[int] = pa.Field(nullable=True, description="First page number")
    is_experimental_doc: Series[bool] = pa.Field(
        nullable=True, description="Is experimental document"
    )
    issue: Series[int] = pa.Field(nullable=True, description="Journal issue number")
    last_page: Series[float] = pa.Field(nullable=True, description="Last page number")
    month: Series[int] = pa.Field(nullable=True, description="Publication month")
    postcodes: Series[str] = pa.Field(nullable=True, description="Postal codes")
    volume: Series[float] = pa.Field(nullable=True, description="Journal volume")
    
    # Enriched fields from external sources
    source: Series[str] = pa.Field(
        isin=["chembl", "crossref", "openalex", "pubmed", "semantic_scholar"],
        description="Data source that provided the enrichment"
    )
    
    # OpenAlex-specific fields
    doi_key: Series[str] = pa.Field(nullable=True, description="DOI key for joining")
    openalex_title: Series[str] = pa.Field(
        nullable=True, description="Title from OpenAlex"
    )
    openalex_doc_type: Series[str] = pa.Field(
        nullable=True, description="Document type from OpenAlex"
    )
    openalex_type_crossref: Series[str] = pa.Field(
        nullable=True, description="CrossRef type from OpenAlex"
    )
    openalex_publication_year: Series[int] = pa.Field(
        nullable=True, description="Publication year from OpenAlex"
    )
    openalex_error: Series[str] = pa.Field(nullable=True, description="Error from OpenAlex")
    
    # Crossref-specific fields
    crossref_title: Series[str] = pa.Field(
        nullable=True, description="Title from Crossref"
    )
    crossref_doc_type: Series[str] = pa.Field(
        nullable=True, description="Document type from Crossref"
    )
    crossref_subject: Series[str] = pa.Field(nullable=True, description="Subject from Crossref")
    crossref_error: Series[str] = pa.Field(nullable=True, description="Error from Crossref")
    
    # Semantic Scholar-specific fields
    scholar_pmid: Series[str] = pa.Field(nullable=True, description="PMID from Semantic Scholar")
    scholar_doi: Series[str] = pa.Field(nullable=True, description="DOI from Semantic Scholar")
    scholar_semantic_scholar_id: Series[str] = pa.Field(
        nullable=True, description="Semantic Scholar ID"
    )
    scholar_publication_types: Series[str] = pa.Field(
        nullable=True, description="Publication types from Semantic Scholar"
    )
    scholar_venue: Series[str] = pa.Field(
        nullable=True, description="Venue from Semantic Scholar"
    )
    scholar_external_ids: Series[str] = pa.Field(
        nullable=True, description="External IDs JSON from Semantic Scholar"
    )
    scholar_error: Series[str] = pa.Field(
        nullable=True, description="Error from Semantic Scholar"
    )
    
    # PubMed-specific fields
    pubmed_pmid: Series[str] = pa.Field(nullable=True, description="PubMed ID")
    pubmed_doi: Series[str] = pa.Field(nullable=True, description="DOI from PubMed")
    pubmed_article_title: Series[str] = pa.Field(
        nullable=True, description="Article title from PubMed"
    )
    pubmed_abstract: Series[str] = pa.Field(nullable=True, description="Abstract from PubMed")
    pubmed_journal_title: Series[str] = pa.Field(
        nullable=True, description="Journal title from PubMed"
    )
    pubmed_volume: Series[str] = pa.Field(nullable=True, description="Volume from PubMed")
    pubmed_issue: Series[str] = pa.Field(nullable=True, description="Issue from PubMed")
    pubmed_start_page: Series[str] = pa.Field(nullable=True, description="Start page from PubMed")
    pubmed_end_page: Series[str] = pa.Field(nullable=True, description="End page from PubMed")
    pubmed_publication_type: Series[str] = pa.Field(
        nullable=True, description="Publication type from PubMed"
    )
    pubmed_mesh_descriptors: Series[str] = pa.Field(
        nullable=True, description="MeSH descriptors from PubMed"
    )
    pubmed_mesh_qualifiers: Series[str] = pa.Field(
        nullable=True, description="MeSH qualifiers from PubMed"
    )
    pubmed_chemical_list: Series[str] = pa.Field(
        nullable=True, description="Chemical list from PubMed"
    )
    pubmed_year_completed: Series[int] = pa.Field(
        nullable=True, description="Year completed from PubMed"
    )
    pubmed_month_completed: Series[int] = pa.Field(
        nullable=True, description="Month completed from PubMed"
    )
    pubmed_day_completed: Series[int] = pa.Field(
        nullable=True, description="Day completed from PubMed"
    )
    pubmed_year_revised: Series[int] = pa.Field(
        nullable=True, description="Year revised from PubMed"
    )
    pubmed_month_revised: Series[int] = pa.Field(
        nullable=True, description="Month revised from PubMed"
    )
    pubmed_day_revised: Series[int] = pa.Field(nullable=True, description="Day revised from PubMed")
    pubmed_issn: Series[str] = pa.Field(nullable=True, description="ISSN from PubMed")
    pubmed_error: Series[str] = pa.Field(nullable=True, description="Error from PubMed")

    class Config:
        strict = True
        coerce = True


class DocumentQCSchema(pa.DataFrameModel):
    """Schema for document QC metrics."""

    metric: Series[str] = pa.Field(description="QC metric name")
    value: Series[int] = pa.Field(ge=0, description="QC metric value")

    class Config:
        strict = True
        coerce = True


__all__ = ["DocumentOutputSchema", "DocumentQCSchema"]
