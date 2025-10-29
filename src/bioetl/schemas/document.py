"""Document schema for multi-source enrichment according to IO_SCHEMAS_AND_DIAGRAMS.md."""

import pandera as pa
from pandera.typing import Series

from bioetl.schemas.base import BaseSchema


class DocumentRawSchema(pa.DataFrameModel):
    """Schema for raw ChEMBL document payloads prior to normalization."""

    document_chembl_id: Series[str] = pa.Field(
        regex=r"^CHEMBL\d+$",
        nullable=False,
        description="Primary identifier for the document record",
    )
    abstract: Series[str] = pa.Field(nullable=True, description="Abstract text from ChEMBL")
    authors: Series[str] = pa.Field(nullable=True, description="Author list from ChEMBL")
    classification: Series[str] = pa.Field(
        nullable=True,
        description="Document classification flag provided by ChEMBL",
    )
    document_contains_external_links: Series[str] = pa.Field(
        nullable=True,
        description="Raw indicator for external references provided by ChEMBL",
    )
    doi: Series[str] = pa.Field(nullable=True, description="Digital Object Identifier")
    first_page: Series[str] = pa.Field(nullable=True, description="First page of article")
    is_experimental_doc: Series[str] = pa.Field(
        nullable=True,
        description="Whether ChEMBL classifies the document as experimental",
    )
    issue: Series[str] = pa.Field(nullable=True, description="Journal issue")
    journal: Series[str] = pa.Field(nullable=True, description="Journal name")
    journal_abbrev: Series[str] = pa.Field(nullable=True, description="Journal abbreviation")
    last_page: Series[str] = pa.Field(nullable=True, description="Last page of article")
    month: Series[str] = pa.Field(nullable=True, description="Publication month reported by ChEMBL")
    pubmed_id: Series[str] = pa.Field(
        nullable=True,
        description="PubMed identifier supplied by ChEMBL",
    )
    title: Series[str] = pa.Field(nullable=True, description="Document title from ChEMBL")
    volume: Series[str] = pa.Field(nullable=True, description="Journal volume")
    year: Series[str] = pa.Field(
        nullable=True,
        description="Publication year reported by ChEMBL",
    )
    source: Series[str] = pa.Field(
        nullable=True,
        description="Source system providing the document payload",
    )

    class Config:
        strict = False
        ordered = True
        coerce = True


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

    # Resolved fields with precedence
    pmid: Series[int] = pa.Field(nullable=True, description="Resolved PMID using precedence")
    pmid_source: Series[str] = pa.Field(nullable=True, description="Source of resolved PMID")
    doi_clean: Series[str] = pa.Field(nullable=True, description="Resolved DOI using precedence")
    doi_clean_source: Series[str] = pa.Field(nullable=True, description="Source of resolved DOI")
    title: Series[str] = pa.Field(nullable=True, description="Resolved title")
    title_source: Series[str] = pa.Field(nullable=True, description="Source of resolved title")
    abstract: Series[str] = pa.Field(nullable=True, description="Resolved abstract")
    abstract_source: Series[str] = pa.Field(nullable=True, description="Source of resolved abstract")
    journal: Series[str] = pa.Field(nullable=True, description="Resolved journal name")
    journal_source: Series[str] = pa.Field(nullable=True, description="Source of resolved journal")
    journal_abbrev: Series[str] = pa.Field(nullable=True, description="Resolved journal abbreviation")
    journal_abbrev_source: Series[str] = pa.Field(nullable=True, description="Source of resolved journal abbreviation")
    authors: Series[str] = pa.Field(nullable=True, description="Resolved authors")
    authors_source: Series[str] = pa.Field(nullable=True, description="Source of resolved authors")
    year: Series[int] = pa.Field(nullable=True, description="Resolved publication year")
    year_source: Series[str] = pa.Field(nullable=True, description="Source of resolved publication year")
    volume: Series[str] = pa.Field(nullable=True, description="Resolved journal volume")
    volume_source: Series[str] = pa.Field(nullable=True, description="Source of resolved journal volume")
    issue: Series[str] = pa.Field(nullable=True, description="Resolved journal issue")
    issue_source: Series[str] = pa.Field(nullable=True, description="Source of resolved journal issue")
    first_page: Series[str] = pa.Field(nullable=True, description="Resolved first page")
    first_page_source: Series[str] = pa.Field(nullable=True, description="Source of resolved first page")
    last_page: Series[str] = pa.Field(nullable=True, description="Resolved last page")
    last_page_source: Series[str] = pa.Field(nullable=True, description="Source of resolved last page")
    issn_print: Series[str] = pa.Field(nullable=True, description="Resolved print ISSN")
    issn_print_source: Series[str] = pa.Field(nullable=True, description="Source of resolved print ISSN")
    issn_electronic: Series[str] = pa.Field(nullable=True, description="Resolved electronic ISSN")
    issn_electronic_source: Series[str] = pa.Field(nullable=True, description="Source of resolved electronic ISSN")
    is_oa: Series[bool] = pa.Field(nullable=True, description="Resolved Open Access flag")
    is_oa_source: Series[str] = pa.Field(nullable=True, description="Source of Open Access flag")
    oa_status: Series[str] = pa.Field(nullable=True, description="Resolved OA status")
    oa_status_source: Series[str] = pa.Field(nullable=True, description="Source of OA status")
    oa_url: Series[str] = pa.Field(nullable=True, description="Resolved OA URL")
    oa_url_source: Series[str] = pa.Field(nullable=True, description="Source of OA URL")
    citation_count: Series[int] = pa.Field(nullable=True, description="Resolved citation count")
    citation_count_source: Series[str] = pa.Field(nullable=True, description="Source of citation count")
    influential_citations: Series[int] = pa.Field(nullable=True, description="Resolved influential citation count")
    influential_citations_source: Series[str] = pa.Field(nullable=True, description="Source of influential citations")
    fields_of_study: Series[str] = pa.Field(nullable=True, description="Resolved fields of study")
    fields_of_study_source: Series[str] = pa.Field(nullable=True, description="Source of fields of study")
    concepts_top3: Series[str] = pa.Field(nullable=True, description="Resolved OpenAlex concepts")
    concepts_top3_source: Series[str] = pa.Field(nullable=True, description="Source of OpenAlex concepts")
    mesh_terms: Series[str] = pa.Field(nullable=True, description="Resolved MeSH terms")
    mesh_terms_source: Series[str] = pa.Field(nullable=True, description="Source of MeSH terms")
    chemicals: Series[str] = pa.Field(nullable=True, description="Resolved chemicals list")
    chemicals_source: Series[str] = pa.Field(nullable=True, description="Source of chemicals list")

    conflict_doi: Series[bool] = pa.Field(nullable=True, description="Conflict flag for DOI discrepancies")
    conflict_pmid: Series[bool] = pa.Field(nullable=True, description="Conflict flag for PMID discrepancies")

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

    # Fallback level error context
    error_type: Series[str] = pa.Field(nullable=True, description="Код ошибки для fallback строки")
    error_message: Series[str] = pa.Field(nullable=True, description="Текст ошибки для fallback строки")
    attempted_at: Series[str] = pa.Field(nullable=True, description="ISO8601 время последней попытки")

    # System fields (from BaseSchema)
    # index, hash_row, hash_business_key, pipeline_version, source_system, chembl_release, extracted_at

    # Column order according to IO_SCHEMAS_AND_DIAGRAMS.md line 957
    # Stored as class attribute to avoid Pandera treating it as a custom check
    _column_order = [
        "index",
        "hash_row",
        "hash_business_key",
        "pipeline_version",
        "source_system",
        "chembl_release",
        "extracted_at",
        "document_chembl_id",
        "document_pubmed_id",
        "document_classification",
        "referenses_on_previous_experiments",
        "original_experimental_document",
        "pmid",
        "pmid_source",
        "doi_clean",
        "doi_clean_source",
        "title",
        "title_source",
        "abstract",
        "abstract_source",
        "journal",
        "journal_source",
        "journal_abbrev",
        "journal_abbrev_source",
        "authors",
        "authors_source",
        "year",
        "year_source",
        "volume",
        "volume_source",
        "issue",
        "issue_source",
        "first_page",
        "first_page_source",
        "last_page",
        "last_page_source",
        "issn_print",
        "issn_print_source",
        "issn_electronic",
        "issn_electronic_source",
        "is_oa",
        "is_oa_source",
        "oa_status",
        "oa_status_source",
        "oa_url",
        "oa_url_source",
        "citation_count",
        "citation_count_source",
        "influential_citations",
        "influential_citations_source",
        "fields_of_study",
        "fields_of_study_source",
        "concepts_top3",
        "concepts_top3_source",
        "mesh_terms",
        "mesh_terms_source",
        "chemicals",
        "chemicals_source",
        "conflict_doi",
        "conflict_pmid",
        "chembl_pmid",
        "pubmed_pmid",
        "openalex_pmid",
        "semantic_scholar_pmid",
        "chembl_title",
        "crossref_title",
        "openalex_title",
        "pubmed_article_title",
        "semantic_scholar_title",
        "chembl_abstract",
        "pubmed_abstract",
        "chembl_authors",
        "crossref_authors",
        "openalex_authors",
        "pubmed_authors",
        "semantic_scholar_authors",
        "chembl_doi",
        "crossref_doi",
        "openalex_doi",
        "pubmed_doi",
        "semantic_scholar_doi",
        "chembl_doc_type",
        "crossref_doc_type",
        "openalex_doc_type",
        "openalex_crossref_doc_type",
        "pubmed_doc_type",
        "semantic_scholar_doc_type",
        "chembl_journal",
        "pubmed_journal",
        "semantic_scholar_journal",
        "chembl_year",
        "openalex_year",
        "chembl_volume",
        "pubmed_volume",
        "chembl_issue",
        "pubmed_issue",
        "pubmed_first_page",
        "pubmed_last_page",
        "openalex_issn",
        "pubmed_issn",
        "semantic_scholar_issn",
        "pubmed_mesh_descriptors",
        "pubmed_mesh_qualifiers",
        "pubmed_chemical_list",
        "pubmed_year_completed",
        "pubmed_month_completed",
        "pubmed_day_completed",
        "pubmed_year_revised",
        "pubmed_month_revised",
        "pubmed_day_revised",
        "crossref_subject",
        "crossref_error",
        "openalex_error",
        "pubmed_error",
        "semantic_scholar_error",
        "error_type",
        "error_message",
        "attempted_at",
    ]

    class Config:
        strict = True
        coerce = True
        ordered = True


class DocumentNormalizedSchema(DocumentSchema):
    """Alias schema for normalized document outputs."""

    class Config(DocumentSchema.Config):
        strict = True
