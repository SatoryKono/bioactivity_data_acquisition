"""Mapping of API fields to output CSV columns for document pipeline."""

from typing import Any

# Mapping for PubMed fields (клиент уже возвращает с префиксом pubmed_)
PUBMED_FIELD_MAPPING = {
    "pubmed_pmid": "pubmed_pmid",
    "pubmed_doi": "pubmed_doi", 
    "pubmed_title": "pubmed_article_title",
    "pubmed_abstract": "pubmed_abstract",
    "pubmed_journal": "pubmed_journal",
    "pubmed_volume": "pubmed_volume",
    "pubmed_issue": "pubmed_issue",
    "pubmed_first_page": "pubmed_first_page",
    "pubmed_last_page": "pubmed_last_page",
    "pubmed_doc_type": "pubmed_doc_type",
    "pubmed_mesh_descriptors": "pubmed_mesh_descriptors",
    "pubmed_mesh_qualifiers": "pubmed_mesh_qualifiers",
    "pubmed_chemical_list": "pubmed_chemical_list",
    "pubmed_year_completed": "pubmed_year_completed",
    "pubmed_month_completed": "pubmed_month_completed",
    "pubmed_day_completed": "pubmed_day_completed",
    "pubmed_year_revised": "pubmed_year_revised",
    "pubmed_month_revised": "pubmed_month_revised",
    "pubmed_day_revised": "pubmed_day_revised",
    "pubmed_issn": "pubmed_issn",
    "pubmed_pmcid": "pubmed_pmcid",
    "pubmed_authors": "pubmed_authors",
    "pubmed_year": "pubmed_year",
    "pubmed_month": "pubmed_month",
    "pubmed_day": "pubmed_day",
    "pubmed_pages": "pubmed_pages",
}

# Mapping for Crossref fields (клиент уже возвращает с префиксом crossref_)
CROSSREF_FIELD_MAPPING = {
    "crossref_doi": "crossref_doi",
    "crossref_title": "crossref_title",
    "crossref_doc_type": "crossref_doc_type",
    "crossref_subject": "crossref_subject",
    "crossref_year": "crossref_year",
    "crossref_month": "crossref_month",
    "crossref_day": "crossref_day",
    "crossref_volume": "crossref_volume",
    "crossref_issue": "crossref_issue",
    "crossref_first_page": "crossref_first_page",
    "crossref_last_page": "crossref_last_page",
    "crossref_journal": "crossref_journal",
    "crossref_authors": "crossref_authors",
    "crossref_issn": "crossref_issn",
    "crossref_isbn": "crossref_isbn",
    "crossref_publisher": "crossref_publisher",
    "crossref_abstract": "crossref_abstract",
}

# Mapping for OpenAlex fields (клиент уже возвращает с префиксом openalex_)
OPENALEX_FIELD_MAPPING = {
    "openalex_doi": "openalex_doi",
    "openalex_title": "openalex_title",
    "openalex_doc_type": "openalex_doc_type",
    "openalex_crossref_doc_type": "openalex_crossref_doc_type",
    "openalex_year": "openalex_year",
    "openalex_month": "openalex_month",
    "openalex_day": "openalex_day",
    "openalex_pmid": "openalex_pmid",
    "openalex_abstract": "openalex_abstract",
    "openalex_issn": "openalex_issn",
    "openalex_authors": "openalex_authors",
    "openalex_concepts": "openalex_concepts",
    "openalex_type": "openalex_type",
    "openalex_journal": "openalex_journal",
    "openalex_volume": "openalex_volume",
    "openalex_issue": "openalex_issue",
    "openalex_first_page": "openalex_first_page",
    "openalex_last_page": "openalex_last_page",
    "openalex_publisher": "openalex_publisher",
}

# Mapping for Semantic Scholar fields (клиент уже возвращает с префиксом semantic_scholar_)
SEMANTIC_SCHOLAR_FIELD_MAPPING = {
    "semantic_scholar_pmid": "semantic_scholar_pmid",
    "semantic_scholar_doi": "semantic_scholar_doi",
    "semantic_scholar_title": "semantic_scholar_title",
    "semantic_scholar_doc_type": "semantic_scholar_doc_type",
    "semantic_scholar_journal": "semantic_scholar_venue",
    "semantic_scholar_authors": "semantic_scholar_authors",
    "semantic_scholar_abstract": "semantic_scholar_abstract",
    "semantic_scholar_year": "semantic_scholar_year",
    "semantic_scholar_citation_count": "semantic_scholar_citation_count",
    "semantic_scholar_external_ids": "semantic_scholar_external_ids",
    "semantic_scholar_issn": "semantic_scholar_issn",
    "semantic_scholar_venue": "semantic_scholar_venue",
}

# Mapping for ChEMBL fields (клиент уже возвращает с префиксом chembl_)
CHEMBL_FIELD_MAPPING = {
    "document_chembl_id": "document_chembl_id",
    "chembl_title": "chembl_title",
    "chembl_journal": "chembl_journal",
    "chembl_volume": "chembl_volume",
    "chembl_issue": "chembl_issue",
    "chembl_year": "chembl_year",
    "chembl_month": "chembl_month",
    "chembl_day": "chembl_day",
    "chembl_doi": "chembl_doi",
    "chembl_pmid": "chembl_pmid",
    "chembl_doc_type": "chembl_doc_type",
    "chembl_abstract": "chembl_abstract",
    "chembl_authors": "chembl_authors",
    "chembl_first_page": "chembl_first_page",
    "chembl_last_page": "chembl_last_page",
    "chembl_issn": "chembl_issn",
}

# Combined mapping for all sources
ALL_FIELD_MAPPINGS = {
    "pubmed": PUBMED_FIELD_MAPPING,
    "crossref": CROSSREF_FIELD_MAPPING,
    "openalex": OPENALEX_FIELD_MAPPING,
    "semantic_scholar": SEMANTIC_SCHOLAR_FIELD_MAPPING,
    "chembl": CHEMBL_FIELD_MAPPING,
}

def apply_field_mapping(record: dict, source: str) -> dict:
    """Apply field mapping for a specific source.
    
    Args:
        record: Raw record from API
        source: Source name (pubmed, crossref, openalex, semantic_scholar, chembl)
        
    Returns:
        Mapped record with correct column names
    """
    if source not in ALL_FIELD_MAPPINGS:
        return record
    
    mapping = ALL_FIELD_MAPPINGS[source]
    mapped_record = {}
    
    for api_field, csv_field in mapping.items():
        # Handle nested fields (e.g., "authors.name")
        if "." in api_field:
            value = _get_nested_value(record, api_field)
        else:
            value = record.get(api_field)
        
        # Convert lists to strings if needed
        if isinstance(value, list):
            if value:
                # Join list elements with semicolon
                value = "; ".join(str(item) for item in value if item is not None)
            else:
                value = None
        
        # Only add the field if it has a value or if it's a key field
        if value is not None or api_field in ["document_chembl_id", "document_pubmed_id", "doi"]:
            mapped_record[csv_field] = value
    
    return mapped_record

def _get_nested_value(record: dict, field_path: str) -> Any:
    """Get value from nested dictionary using dot notation.
    
    Args:
        record: Dictionary to search in
        field_path: Dot-separated path (e.g., "authors.name")
        
    Returns:
        Value at the specified path or None
    """
    keys = field_path.split(".")
    value = record
    
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return None
    
    return value
