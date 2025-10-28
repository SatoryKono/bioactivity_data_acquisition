"""Document schema for multi-source enrichment according to IO_SCHEMAS_AND_DIAGRAMS.md."""

import pandera as pa
from pandera.typing import Series

from bioetl.schemas.base import BaseSchema


class DocumentSchema(BaseSchema):
    """Unified schema for ChEMBL documents with multi-source enrichment.

    Primary Key: [document_chembl_id]
    Supports enrichment from 5 sources: ChEMBL, PubMed, Crossref, OpenAlex, Semantic Scholar
    ~70 fields with prefixed naming convention
    """

    # Primary Key
    document_chembl_id: Series[str] = pa.Field(
        nullable=False, description="Первичный ключ"
    )

    # Core document information
    document_pubmed_id: Series[int] = pa.Field(nullable=True, description="Document PubMed ID")
    document_classification: Series[str] = pa.Field(nullable=True, description="Document classification")
    referenses_on_previous_experiments: Series[bool] = pa.Field(nullable=True, description="References on previous experiments")
    original_experimental_document: Series[bool] = pa.Field(nullable=True, description="Original experimental document")

    # PMID fields (4 sources)
    chembl_pmid: Series[int] = pa.Field(nullable=True, description="PMID из ChEMBL")
    pubmed_pmid: Series[int] = pa.Field(nullable=True, description="PMID из PubMed")
    openalex_pmid: Series[int] = pa.Field(nullable=True, description="PMID из OpenAlex")
    semantic_scholar_pmid: Series[int] = pa.Field(nullable=True, description="PMID из Semantic Scholar")

    # Title fields (5 sources)
    chembl_title: Series[str] = pa.Field(nullable=True, description="Заголовок из ChEMBL")
    crossref_title: Series[str] = pa.Field(nullable=True, description="Заголовок из Crossref")
    openalex_title: Series[str] = pa.Field(nullable=True, description="Заголовок из OpenAlex")
    pubmed_article_title: Series[str] = pa.Field(nullable=True, description="Заголовок из PubMed")
    semantic_scholar_title: Series[str] = pa.Field(nullable=True, description="Заголовок из Semantic Scholar")

    # Abstract fields (2 sources)
    chembl_abstract: Series[str] = pa.Field(nullable=True, description="Аннотация из ChEMBL")
    pubmed_abstract: Series[str] = pa.Field(nullable=True, description="Аннотация из PubMed")

    # Authors fields (5 sources)
    chembl_authors: Series[str] = pa.Field(nullable=True, description="Авторы из ChEMBL")
    crossref_authors: Series[str] = pa.Field(nullable=True, description="Авторы из Crossref")
    openalex_authors: Series[str] = pa.Field(nullable=True, description="Авторы из OpenAlex")
    pubmed_authors: Series[str] = pa.Field(nullable=True, description="Авторы из PubMed")
    semantic_scholar_authors: Series[str] = pa.Field(nullable=True, description="Авторы из Semantic Scholar")

    # DOI fields (5 sources)
    chembl_doi: Series[str] = pa.Field(nullable=True, description="DOI из ChEMBL")
    crossref_doi: Series[str] = pa.Field(nullable=True, description="DOI из Crossref")
    openalex_doi: Series[str] = pa.Field(nullable=True, description="DOI из OpenAlex")
    pubmed_doi: Series[str] = pa.Field(nullable=True, description="DOI из PubMed")
    semantic_scholar_doi: Series[str] = pa.Field(nullable=True, description="DOI из Semantic Scholar")

    # Doc Type fields (6 sources)
    chembl_doc_type: Series[str] = pa.Field(nullable=True, description="Doc type из ChEMBL")
    crossref_doc_type: Series[str] = pa.Field(nullable=True, description="Doc type из Crossref")
    openalex_doc_type: Series[str] = pa.Field(nullable=True, description="Doc type из OpenAlex")
    openalex_crossref_doc_type: Series[str] = pa.Field(nullable=True, description="Doc type OpenAlex→Crossref")
    pubmed_doc_type: Series[str] = pa.Field(nullable=True, description="Doc type из PubMed")
    semantic_scholar_doc_type: Series[str] = pa.Field(nullable=True, description="Doc type из Semantic Scholar")

    # Journal fields (3 sources)
    chembl_journal: Series[str] = pa.Field(nullable=True, description="Название журнала из ChEMBL")
    pubmed_journal: Series[str] = pa.Field(nullable=True, description="Название журнала из PubMed")
    semantic_scholar_journal: Series[str] = pa.Field(nullable=True, description="Название журнала из Semantic Scholar")

    # Year fields (2 sources)
    chembl_year: Series[int] = pa.Field(
        ge=1800, le=2100, nullable=True, description="Год публикации из ChEMBL"
    )
    openalex_year: Series[int] = pa.Field(nullable=True, description="Год публикации из OpenAlex")

    # Volume/Issue fields (4 sources)
    chembl_volume: Series[str] = pa.Field(nullable=True, description="Том журнала из ChEMBL")
    pubmed_volume: Series[str] = pa.Field(nullable=True, description="Том журнала из PubMed")
    chembl_issue: Series[str] = pa.Field(nullable=True, description="Выпуск журнала из ChEMBL")
    pubmed_issue: Series[str] = pa.Field(nullable=True, description="Выпуск журнала из PubMed")

    # Pages fields (2 sources)
    pubmed_first_page: Series[str] = pa.Field(nullable=True, description="Первая страница из PubMed")
    pubmed_last_page: Series[str] = pa.Field(nullable=True, description="Последняя страница из PubMed")

    # ISSN fields (3 sources)
    openalex_issn: Series[str] = pa.Field(nullable=True, description="ISSN из OpenAlex")
    pubmed_issn: Series[str] = pa.Field(nullable=True, description="ISSN из PubMed")
    semantic_scholar_issn: Series[str] = pa.Field(nullable=True, description="ISSN из Semantic Scholar")

    # PubMed specific metadata
    pubmed_mesh_descriptors: Series[str] = pa.Field(nullable=True, description="PubMed MeSH descriptors")
    pubmed_mesh_qualifiers: Series[str] = pa.Field(nullable=True, description="PubMed MeSH qualifiers")
    pubmed_chemical_list: Series[str] = pa.Field(nullable=True, description="PubMed chemical list")
    pubmed_year_completed: Series[int] = pa.Field(nullable=True, description="PubMed year completed")
    pubmed_month_completed: Series[int] = pa.Field(nullable=True, description="PubMed month completed")
    pubmed_day_completed: Series[int] = pa.Field(nullable=True, description="PubMed day completed")
    pubmed_year_revised: Series[int] = pa.Field(nullable=True, description="PubMed year revised")
    pubmed_month_revised: Series[int] = pa.Field(nullable=True, description="PubMed month revised")
    pubmed_day_revised: Series[int] = pa.Field(nullable=True, description="PubMed day revised")

    # Crossref specific
    crossref_subject: Series[str] = pa.Field(nullable=True, description="Crossref subject")

    # Error tracking fields (4 sources)
    crossref_error: Series[str] = pa.Field(nullable=True, description="Ошибка Crossref адаптера")
    openalex_error: Series[str] = pa.Field(nullable=True, description="Ошибка OpenAlex адаптера")
    pubmed_error: Series[str] = pa.Field(nullable=True, description="Ошибка PubMed адаптера")
    semantic_scholar_error: Series[str] = pa.Field(nullable=True, description="Ошибка Semantic Scholar адаптера")

    # System fields (from BaseSchema)
    # index, hash_row, hash_business_key, pipeline_version, source_system, chembl_release, extracted_at

    # Column order according to IO_SCHEMAS_AND_DIAGRAMS.md line 957
    # Stored as class attribute to avoid Pandera treating it as a custom check
    column_order = [
            "index",
            "extracted_at",
            "pipeline_version",
            "source_system",
            "chembl_release",
            "hash_business_key",
            "hash_row",
            "document_chembl_id",
            "document_pubmed_id",
            "document_classification",
            "referenses_on_previous_experiments",
            "original_experimental_document",
            "pubmed_mesh_descriptors",
            "pubmed_mesh_qualifiers",
            "pubmed_chemical_list",
            "crossref_subject",
            # PMID (4)
            "chembl_pmid",
            "openalex_pmid",
            "pubmed_pmid",
            "semantic_scholar_pmid",
            # Title (5)
            "chembl_title",
            "crossref_title",
            "openalex_title",
            "pubmed_article_title",
            "semantic_scholar_title",
            # Abstract (2)
            "chembl_abstract",
            "pubmed_abstract",
            # Authors (5)
            "chembl_authors",
            "crossref_authors",
            "openalex_authors",
            "pubmed_authors",
            "semantic_scholar_authors",
            # DOI (5)
            "chembl_doi",
            "crossref_doi",
            "openalex_doi",
            "pubmed_doi",
            "semantic_scholar_doi",
            # Doc Type (6)
            "chembl_doc_type",
            "crossref_doc_type",
            "openalex_doc_type",
            "openalex_crossref_doc_type",
            "pubmed_doc_type",
            "semantic_scholar_doc_type",
            # ISSN (3) - in spec comes after Journal, before Year
            "openalex_issn",
            "pubmed_issn",
            "semantic_scholar_issn",
            # Journal (3)
            "chembl_journal",
            "pubmed_journal",
            "semantic_scholar_journal",
            # Year (2)
            "chembl_year",
            "openalex_year",
            # Volume/Issue (4)
            "chembl_volume",
            "pubmed_volume",
            "chembl_issue",
            "pubmed_issue",
            # Pages (2)
            "pubmed_first_page",
            "pubmed_last_page",
            # Errors (4) - should be before pubmed dates
            "crossref_error",
            "openalex_error",
            "pubmed_error",
            "semantic_scholar_error",
            # PubMed dates (6)
            "pubmed_year_completed",
            "pubmed_month_completed",
            "pubmed_day_completed",
            "pubmed_year_revised",
            "pubmed_month_revised",
            "pubmed_day_revised",
            # Note: pipeline_version, source_system, chembl_release from BaseSchema
            # but not in column_order spec line 957
        ]

    class Config:
        strict = True
        coerce = True
        ordered = True
