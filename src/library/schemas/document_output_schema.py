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
    document_pubmed_id: Series[str] = pa.Field(nullable=True, description="PubMed identifier")
    pubmed_id: Series[str] = pa.Field(nullable=True, description="PubMed identifier (legacy field)")
    journal: Series[str] = pa.Field(nullable=True, description="Journal name")
    year: Series[float] = pa.Field(nullable=True, description="Publication year")
    
    # Legacy ChEMBL fields (keeping for backward compatibility)
    abstract: Series[str] = pa.Field(nullable=True, description="Document abstract")
    authors: Series[str] = pa.Field(nullable=True, description="Document authors")
    pubmed_authors: Series[str] = pa.Field(
        nullable=True, description="Document authors from PubMed"
    )
    document_classification: Series[str] = pa.Field(nullable=True, description="Document classification")
    classification: Series[str] = pa.Field(nullable=True, description="Document classification (legacy field)")
    document_contains_external_links: Series[bool] = pa.Field(
        nullable=True, description="Contains external links (legacy field)"
    )
    referenses_on_previous_experiments: Series[bool] = pa.Field(
        nullable=True, description="Contains external links"
    )
    first_page: Series[int] = pa.Field(nullable=True, description="First page number")
    original_experimental_document: Series[bool] = pa.Field(
        nullable=True, description="Is experimental document"
    )
    is_experimental_doc: Series[bool] = pa.Field(
        nullable=True, description="Is experimental document (legacy field)"
    )
    issue: Series[str] = pa.Field(nullable=True, description="Journal issue number")
    last_page: Series[float] = pa.Field(nullable=True, description="Last page number")
    month: Series[int] = pa.Field(nullable=True, description="Publication month")
    volume: Series[float] = pa.Field(nullable=True, description="Journal volume")
    
    # Enriched fields from external sources
    # source column removed - not needed in final output
    
    # OpenAlex-specific fields (только те, что реально создаются клиентом)
    openalex_doi: Series[str] = pa.Field(nullable=True, description="DOI from OpenAlex")
    openalex_title: Series[str] = pa.Field(
        nullable=True, description="Title from OpenAlex"
    )
    openalex_doc_type: Series[str] = pa.Field(
        nullable=True, description="Document type from OpenAlex"
    )
    openalex_crossref_doc_type: Series[str] = pa.Field(
        nullable=True, description="CrossRef document type from OpenAlex"
    )
    openalex_year: Series[int] = pa.Field(
        nullable=True, description="Publication year from OpenAlex"
    )
    openalex_authors: Series[str] = pa.Field(nullable=True, description="Authors from OpenAlex")
    openalex_issn: Series[str] = pa.Field(nullable=True, description="ISSN from OpenAlex")
    openalex_pmid: Series[str] = pa.Field(nullable=True, description="PMID from OpenAlex")
    openalex_error: Series[str] = pa.Field(nullable=True, description="Error from OpenAlex")
    
    # Crossref-specific fields (только те, что реально создаются клиентом)
    crossref_doi: Series[str] = pa.Field(nullable=True, description="DOI from Crossref")
    crossref_title: Series[str] = pa.Field(
        nullable=True, description="Title from Crossref"
    )
    crossref_doc_type: Series[str] = pa.Field(
        nullable=True, description="Document type from Crossref"
    )
    crossref_subject: Series[str] = pa.Field(nullable=True, description="Subject from Crossref")
    crossref_authors: Series[str] = pa.Field(nullable=True, description="Authors from Crossref")
    crossref_error: Series[str] = pa.Field(nullable=True, description="Error from Crossref")
    
    # Semantic Scholar-specific fields (согласно таблице)
    semantic_scholar_pmid: Series[str] = pa.Field(
        nullable=True, description="PMID from Semantic Scholar"
    )
    semantic_scholar_doi: Series[str] = pa.Field(
        nullable=True, description="DOI from Semantic Scholar"
    )
    semantic_scholar_semantic_scholar_id: Series[str] = pa.Field(
        nullable=True, description="Semantic Scholar ID"
    )
    semantic_scholar_doc_type: Series[str] = pa.Field(
        nullable=True, description="Document type from Semantic Scholar"
    )
    semantic_scholar_journal: Series[str] = pa.Field(
        nullable=True, description="Venue from Semantic Scholar"
    )
    semantic_scholar_external_ids: Series[str] = pa.Field(
        nullable=True, description="External IDs JSON from Semantic Scholar"
    )
    semantic_scholar_abstract: Series[str] = pa.Field(
        nullable=True, description="Abstract from Semantic Scholar"
    )
    semantic_scholar_issn: Series[str] = pa.Field(
        nullable=True, description="ISSN from Semantic Scholar"
    )
    semantic_scholar_authors: Series[str] = pa.Field(
        nullable=True, description="Authors from Semantic Scholar"
    )
    semantic_scholar_error: Series[str] = pa.Field(
        nullable=True, description="Error from Semantic Scholar"
    )
    
    # PubMed-specific fields (согласно таблице)
    pubmed_pmid: Series[str] = pa.Field(nullable=True, description="PubMed ID")
    pubmed_doi: Series[str] = pa.Field(nullable=True, description="DOI from PubMed")
    pubmed_article_title: Series[str] = pa.Field(
        nullable=True, description="Article title from PubMed"
    )
    pubmed_abstract: Series[str] = pa.Field(nullable=True, description="Abstract from PubMed")
    pubmed_journal: Series[str] = pa.Field(
        nullable=True, description="Journal from PubMed"
    )
    pubmed_volume: Series[str] = pa.Field(nullable=True, description="Volume from PubMed")
    pubmed_issue: Series[str] = pa.Field(nullable=True, description="Issue from PubMed")
    pubmed_first_page: Series[str] = pa.Field(nullable=True, description="First page from PubMed")
    pubmed_last_page: Series[str] = pa.Field(nullable=True, description="Last page from PubMed")
    pubmed_doc_type: Series[str] = pa.Field(
        nullable=True, description="Document type from PubMed"
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
    
    # ChemBL-specific fields (только те, что реально создаются клиентом)
    chembl_title: Series[str] = pa.Field(nullable=True, description="Title from ChemBL")
    chembl_doi: Series[str] = pa.Field(nullable=True, description="DOI from ChemBL")
    chembl_pmid: Series[str] = pa.Field(nullable=True, description="PubMed ID from ChemBL")
    chembl_journal: Series[str] = pa.Field(nullable=True, description="Journal from ChemBL")
    chembl_year: Series[str] = pa.Field(nullable=True, description="Year from ChemBL")
    chembl_volume: Series[str] = pa.Field(nullable=True, description="Volume from ChemBL")
    chembl_issue: Series[str] = pa.Field(nullable=True, description="Issue from ChemBL")
    chembl_abstract: Series[str] = pa.Field(nullable=True, description="Abstract from ChemBL")
    chembl_authors: Series[str] = pa.Field(nullable=True, description="Authors from ChemBL")
    chembl_first_page: Series[str] = pa.Field(nullable=True, description="First page from ChemBL")
    chembl_last_page: Series[str] = pa.Field(nullable=True, description="Last page from ChemBL")
    source_system: Series[str] = pa.Field(nullable=True, description="Source system identifier")
    extracted_at: Series[str] = pa.Field(nullable=True, description="Timestamp when data was extracted")
    # ChEMBL fields that are not created by the client are removed
    
    # Citation field - removed as it's not created by the pipeline
    
    # Additional fields that appear in the data
    doi_key: Series[str] = pa.Field(nullable=True, description="DOI key from Crossref")
    openalex_type: Series[str] = pa.Field(nullable=True, description="Type from OpenAlex")
    openalex_concepts: Series[str] = pa.Field(nullable=True, description="Concepts from OpenAlex")
    pubmed_title: Series[str] = pa.Field(nullable=True, description="Title from PubMed")
    pubmed_year: Series[str] = pa.Field(nullable=True, description="Year from PubMed")
    pubmed_month: Series[str] = pa.Field(nullable=True, description="Month from PubMed")
    pubmed_day: Series[str] = pa.Field(nullable=True, description="Day from PubMed")
    pubmed_pages: Series[str] = pa.Field(nullable=True, description="Pages from PubMed")
    pubmed_pmcid: Series[str] = pa.Field(nullable=True, description="PMCID from PubMed")
    semantic_scholar_title: Series[str] = pa.Field(nullable=True, description="Title from Semantic Scholar")
    semantic_scholar_venue: Series[str] = pa.Field(nullable=True, description="Venue from Semantic Scholar")
    semantic_scholar_year: Series[str] = pa.Field(nullable=True, description="Year from Semantic Scholar")
    semantic_scholar_citation_count: Series[str] = pa.Field(nullable=True, description="Citation count from Semantic Scholar")
    chembl_doc_type: Series[str] = pa.Field(nullable=True, description="Document type from ChemBL")
    publication_date: Series[str] = pa.Field(nullable=True, description="Publication date")
    document_sortorder: Series[str] = pa.Field(nullable=True, description="Document sort order")
    citation: Series[str] = pa.Field(nullable=True, description="Citation string")
    
    # Validation fields - removed as they are not created by the pipeline

    class Config:
        strict = True
        coerce = True


class DocumentQCSchema(pa.DataFrameModel):
    """Schema for document QC metrics."""

    metric: Series[str] = pa.Field(description="QC metric name")
    value: Series[int] = pa.Field(ge=0, description="QC metric value")

    class Config:
        strict = False  # Allow additional columns not defined in schema
        coerce = True


__all__ = ["DocumentOutputSchema", "DocumentQCSchema"]
