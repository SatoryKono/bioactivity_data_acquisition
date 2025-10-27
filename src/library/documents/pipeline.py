"""Simplified document ETL pipeline orchestration."""

from __future__ import annotations

import hashlib
import logging
import os
import re
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from library.clients.chembl_document import ChEMBLClient
from library.clients.crossref import CrossrefClient
from library.clients.openalex import OpenAlexClient
from library.clients.pubmed import PubMedClient
from library.clients.semantic_scholar import SemanticScholarClient
from library.common.pipeline_base import PipelineBase
from library.config import APIClientConfig
from library.documents.config import DocumentConfig
from library.documents.diagnostics import DocumentDiagnostics
from library.documents.normalize import DocumentNormalizer
from library.documents.quality import DocumentQualityFilter
from library.documents.validate import DocumentValidator
from library.etl.enhanced_correlation import (
    build_correlation_insights,
    build_enhanced_correlation_analysis,
    build_enhanced_correlation_reports,
    prepare_data_for_correlation_analysis,
)
from library.tools.citation_formatter import add_citation_column
from library.tools.journal_normalizer import normalize_journal_columns


class DocumentPipelineError(RuntimeError):
    """Base class for document pipeline errors."""


class DocumentValidationError(DocumentPipelineError):
    """Raised when the input data does not meet schema expectations."""


class DocumentHTTPError(DocumentPipelineError):
    """Raised when upstream HTTP requests fail irrecoverably."""


class DocumentQCError(DocumentPipelineError):
    """Raised when QC checks do not pass configured thresholds."""


class DocumentIOError(DocumentPipelineError):
    """Raised when reading or writing files fails."""


@dataclass(slots=True)
class DocumentETLResult:
    """Container for ETL artefacts."""

    documents: pd.DataFrame
    qc: pd.DataFrame
    meta: dict[str, Any]
    correlation_analysis: dict[str, Any] | None = None
    correlation_reports: dict[str, pd.DataFrame] | None = None
    correlation_insights: list[dict[str, Any]] | None = None


_REQUIRED_COLUMNS = {"document_chembl_id", "doi", "title"}

# Get logger for this module
logger = logging.getLogger(__name__)


def _create_api_client(source: str, config: DocumentConfig) -> Any:
    """Create an API client for the specified source."""
    from library.config import RateLimitSettings, RetrySettings
    
    # Get source-specific configuration
    source_config = config.sources.get(source)
    if not source_config:
        raise DocumentValidationError(f"Source '{source}' not found in configuration")
    
    # Use source-specific timeout or fallback to global
    timeout = source_config.http.timeout_sec or config.http.global_.timeout_sec
    if source == "chembl":
        timeout = max(timeout, 60.0)  # At least 60 seconds for ChEMBL
    
    # Merge headers: default + global + source-specific
    default_headers = _get_headers(source)
    headers = {**default_headers, **config.http.global_.headers, **source_config.http.headers}
    
    # Process secret placeholders in headers
    processed_headers = {}
    for key, value in headers.items():
        if isinstance(value, str):
            def replace_placeholder(match):
                secret_name = match.group(1)
                env_var = os.environ.get(secret_name.upper())
                return env_var if env_var is not None else match.group(0)
            processed_value = re.sub(r'\{([^}]+)\}', replace_placeholder, value)
            # Only include header if the value is not empty after processing and not a placeholder
            if (processed_value and processed_value.strip() and 
                not processed_value.startswith('{') and not processed_value.endswith('}')):
                processed_headers[key] = processed_value
        else:
            processed_headers[key] = value
    headers = processed_headers
    
    # Use source-specific base_url or fallback to default
    base_url = source_config.http.base_url or _get_base_url(source)
    
    # Use source-specific retry settings or fallback to global
    retry_settings = RetrySettings(
        total=source_config.http.retries.get('total', config.http.global_.retries.total),
        backoff_multiplier=source_config.http.retries.get('backoff_multiplier', config.http.global_.retries.backoff_multiplier)
    )
    
    # Create rate limit settings if configured
    rate_limit = None
    if source_config.rate_limit:
        # Convert various rate limit formats to max_calls/period
        max_calls = source_config.rate_limit.get('max_calls')
        period = source_config.rate_limit.get('period')
        
        # If not in max_calls/period format, try to convert from other formats
        if max_calls is None or period is None:
            requests_per_second = source_config.rate_limit.get('requests_per_second')
            if requests_per_second is not None:
                max_calls = 1
                period = 1.0 / requests_per_second
            else:
                # Skip rate limiting if we can't determine the format
                rate_limit = None
        
        # Create RateLimitSettings object if we have valid max_calls and period
        if max_calls is not None and period is not None:
            rate_limit = RateLimitSettings(max_calls=max_calls, period=period)
    
    # Create base API client config
    api_config = APIClientConfig(
        name=source,
        base_url=base_url,
        headers=headers,
        timeout=timeout,
        retries=retry_settings,
        rate_limit=rate_limit,
    )
   
    if source == "chembl":
        return ChEMBLClient(api_config, timeout=timeout)
    elif source == "crossref":
        return CrossrefClient(api_config, timeout=timeout)
    elif source == "openalex":
        return OpenAlexClient(api_config, timeout=timeout)
    elif source == "pubmed":
        return PubMedClient(api_config, timeout=timeout)
    elif source == "semantic_scholar":
        return SemanticScholarClient(api_config, timeout=timeout)
    else:
        raise DocumentValidationError(f"Unsupported source: {source}")


def _get_base_url(source: str) -> str:
    """Get the base URL for the specified source."""
    urls = {
        "chembl": "https://www.ebi.ac.uk/chembl/api/data",
        "crossref": "https://api.crossref.org/works",
        "openalex": "https://api.openalex.org",
        "pubmed": "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/",
        "semantic_scholar": "https://api.semanticscholar.org/graph/v1",
    }
    return urls.get(source, "")


def _get_headers(source: str) -> dict[str, str]:
    """Get default headers for the specified source."""
    headers = {
        "User-Agent": "bioactivity-data-acquisition/0.1.0",
        "Accept": "application/json",  # All sources should accept JSON
    }
    
    return headers


def _extract_data_from_source(
    source: str, 
    client: Any, 
    frame: pd.DataFrame, 
    config: DocumentConfig
) -> pd.DataFrame:
    """Extract data from a specific source."""
    enriched_data = []
    
    # Define default values for all possible columns to ensure they exist
    default_columns = {
        # ChEMBL columns
        "chembl_title": None, "chembl_doi": None, "chembl_pmid": None,
        "chembl_journal": None, "chembl_year": None, "chembl_volume": None, 
        "chembl_issue": None,
        # Crossref columns  
        "crossref_doi": None, "crossref_title": None, "crossref_doc_type": None, "crossref_subject": None, 
        "crossref_error": None,
        # OpenAlex columns
        "openalex_doi": None, "openalex_title": None, "openalex_doc_type": None,
        "openalex_crossref_doc_type": None, "openalex_year": None, 
        "openalex_error": None,
        # PubMed columns
        "pubmed_pmid": None, "pubmed_doi": None, "pubmed_title": None, 
        "pubmed_abstract": None, "pubmed_journal": None, "pubmed_volume": None, 
        "pubmed_issue": None, "pubmed_first_page": None, "pubmed_last_page": None, 
        "pubmed_doc_type": None, "pubmed_mesh_descriptors": None, 
        "pubmed_mesh_qualifiers": None, "pubmed_chemical_list": None,
        "pubmed_year_completed": None, "pubmed_month_completed": None, 
        "pubmed_day_completed": None, "pubmed_year_revised": None, 
        "pubmed_month_revised": None, "pubmed_day_revised": None,
        "pubmed_issn": None, "pubmed_error": None,
        # Semantic Scholar columns
        "semantic_scholar_pmid": None, "semantic_scholar_doi": None, 
        "semantic_scholar_semantic_scholar_id": None, "semantic_scholar_title": None,
        "semantic_scholar_doc_type": None, "semantic_scholar_journal": None,
        "semantic_scholar_external_ids": None, "semantic_scholar_abstract": None,
        "semantic_scholar_issn": None, "semantic_scholar_authors": None,
        "semantic_scholar_error": None,
        # Common columns
        "chembl_doc_type": None, "doi_key": None
    }
    
    for _, row in frame.iterrows():
        try:
            # Start with the original row data and ensure all columns exist
            row_data = row.to_dict()
            # Only add missing columns with default values, don't overwrite existing data
            for key, default_value in default_columns.items():
                if key not in row_data:
                    row_data[key] = default_value
            
            if source == "chembl":
                if (pd.notna(row.get("document_chembl_id")) and 
                    str(row["document_chembl_id"]).strip()):
                    data = client.fetch_by_doc_id(str(row["document_chembl_id"]).strip())
                    # Remove source from data to avoid overwriting
                    data.pop("source", None)
                    # Only update non-None values to preserve existing data
                    for key, value in data.items():
                        if value is not None:
                            row_data[key] = value
                    
            elif source == "crossref":
                if pd.notna(row.get("doi")) and str(row["doi"]).strip():
                    data = client.fetch_by_doi(str(row["doi"]).strip())
                    data.pop("source", None)
                    for key, value in data.items():
                        # Always include error fields and non-None values
                        if value is not None or key.endswith("_error") or key == "degraded":
                            row_data[key] = value
                elif pd.notna(row.get("document_pubmed_id")) and str(row["document_pubmed_id"]).strip():
                    data = client.fetch_by_pmid(str(row["document_pubmed_id"]).strip())
                    data.pop("source", None)
                    for key, value in data.items():
                        # Always include error fields and non-None values
                        if value is not None or key.endswith("_error") or key == "degraded":
                            row_data[key] = value
                    
            elif source == "openalex":
                if pd.notna(row.get("doi")) and str(row["doi"]).strip():
                    data = client.fetch_by_doi(str(row["doi"]).strip())
                    data.pop("source", None)
                    for key, value in data.items():
                        if value is not None:
                            row_data[key] = value
                elif pd.notna(row.get("document_pubmed_id")) and str(row["document_pubmed_id"]).strip():
                    data = client.fetch_by_pmid(str(row["document_pubmed_id"]).strip())
                    data.pop("source", None)
                    for key, value in data.items():
                        if value is not None:
                            row_data[key] = value
                    
            elif source == "pubmed":
                if pd.notna(row.get("document_pubmed_id")) and str(row["document_pubmed_id"]).strip():
                    data = client.fetch_by_pmid(str(row["document_pubmed_id"]).strip())
                    data.pop("source", None)
                    for key, value in data.items():
                        if value is not None:
                            row_data[key] = value
                    
            elif source == "semantic_scholar":
                if pd.notna(row.get("document_pubmed_id")) and str(row["document_pubmed_id"]).strip():
                    # Передаем title для fallback поиска
                    title = row.get("document_title") if pd.notna(row.get("document_title")) else None
                    data = client.fetch_by_pmid(str(row["document_pubmed_id"]).strip(), title=title)
                    data.pop("source", None)
                    for key, value in data.items():
                        if value is not None:
                            row_data[key] = value
            
            enriched_data.append(row_data)
                    
        except Exception as exc:
            # Log error but continue processing other records
            doc_id = row.get('document_chembl_id', 'unknown')
            logger.error(f"Error extracting data from {source} for row {doc_id}: {exc}")
            logger.error(f"Error type: {type(exc).__name__}")
            
            # Ensure error row also has all columns
            error_row = row.to_dict()
            error_row.update(default_columns)
            
            # Set error flag for this source with more detailed error info
            error_msg = f"{type(exc).__name__}: {str(exc)}"
            
            # Специальная обработка для ошибок rate limiting
            if ("429" in str(exc) or "Rate limited" in str(exc) or 
                "RateLimitError" in str(type(exc).__name__)):
                error_msg = f"Rate limited by API: {str(exc)}"
                logger.warning(f"Rate limiting detected for {source}, continuing with next record...")
            
            if source == "crossref":
                error_row["crossref_error"] = error_msg
            elif source == "openalex":
                error_row["openalex_error"] = error_msg
            elif source == "pubmed":
                error_row["pubmed_error"] = error_msg
            elif source == "semantic_scholar":
                error_row["semantic_scholar_error"] = error_msg
            elif source == "chembl":
                error_row["chembl_error"] = error_msg
                
            enriched_data.append(error_row)
    
    return pd.DataFrame(enriched_data)


def _extract_data_from_source_batch(
    source: str, 
    client: Any, 
    frame: pd.DataFrame, 
    config: DocumentConfig
) -> pd.DataFrame:
    """Extract data from a specific source using batch processing where available."""
    
    # Collect identifiers for batch processing
    identifiers = _collect_identifiers(frame, source)
    
    if not identifiers:
        logger.info(f"No identifiers found for {source}, skipping batch processing")
        return frame
    
    # Get batch size for this source
    batch_size = getattr(config.runtime, f"{source}_batch_size", 50)
    
    logger.info(f"Processing {len(identifiers)} identifiers for {source} in batches of {batch_size}")
    
    # Group identifiers by type for batch processing
    doi_identifiers = [id_val for id_type, id_val in identifiers if id_type == "doi"]
    pmid_identifiers = [id_val for id_type, id_val in identifiers if id_type == "pmid"]
    doc_identifiers = [id_val for id_type, id_val in identifiers if id_type == "document_chembl_id"]
    
    batch_results = {}
    
    try:
        if source == "chembl" and doc_identifiers:
            # Process ChEMBL documents in batches
            for batch_ids in _chunk_list(doc_identifiers, batch_size):
                batch_data = client.fetch_documents_batch(batch_ids, batch_size)
                batch_results.update(batch_data)
                
        elif source == "pubmed" and pmid_identifiers:
            # Use existing batch method for PubMed
            for batch_ids in _chunk_list(pmid_identifiers, batch_size):
                batch_data = client.fetch_by_pmids(batch_ids)
                batch_results.update(batch_data)
                
        elif source == "crossref":
            # Process DOIs and PMIDs separately
            if doi_identifiers:
                for batch_ids in _chunk_list(doi_identifiers, batch_size):
                    batch_data = client.fetch_by_dois_batch(batch_ids, batch_size)
                    batch_results.update(batch_data)
            if pmid_identifiers:
                for batch_ids in _chunk_list(pmid_identifiers, batch_size):
                    batch_data = client.fetch_by_pmids_batch(batch_ids, batch_size)
                    batch_results.update(batch_data)
                    
        elif source == "openalex":
            # Process DOIs and PMIDs separately
            if doi_identifiers:
                for batch_ids in _chunk_list(doi_identifiers, batch_size):
                    batch_data = client.fetch_by_dois_batch(batch_ids, batch_size)
                    batch_results.update(batch_data)
            if pmid_identifiers:
                for batch_ids in _chunk_list(pmid_identifiers, batch_size):
                    batch_data = client.fetch_by_pmids_batch(batch_ids, batch_size)
                    batch_results.update(batch_data)
                    
        elif source == "semantic_scholar" and pmid_identifiers:
            # Process Semantic Scholar PMIDs in batches
            for batch_ids in _chunk_list(pmid_identifiers, batch_size):
                batch_data = client.fetch_by_pmids_batch(batch_ids, batch_size)
                batch_results.update(batch_data)
        
        logger.info(f"Successfully processed {len(batch_results)} records from {source}")
        
    except Exception as e:
        logger.warning(f"Batch processing failed for {source}: {e}")
        logger.info(f"Falling back to individual processing for {source}")
        # Fallback to individual processing
        return _extract_data_from_source(source, client, frame, config)
    
    # Merge batch results with original frame
    return _merge_batch_results(frame, batch_results, source)


def read_document_input(path: Path) -> pd.DataFrame:
    """Load the input CSV containing document identifiers."""

    try:
        frame = pd.read_csv(path)
    except FileNotFoundError as exc:
        raise DocumentIOError(f"Input CSV not found: {path}") from exc
    except pd.errors.EmptyDataError as exc:
        raise DocumentValidationError("Input CSV is empty") from exc
    except OSError as exc:  # pragma: no cover - filesystem-level errors are rare
        raise DocumentIOError(f"Failed to read input CSV: {exc}") from exc
    return frame


def _normalise_columns(frame: pd.DataFrame) -> pd.DataFrame:
    normalised = frame.copy()
    
    # Remove postcodes column if it exists (deprecated field)
    if 'postcodes' in normalised.columns:
        normalised = normalised.drop(columns=['postcodes'])
    
    present = {column for column in normalised.columns}
    missing = _REQUIRED_COLUMNS - present
    if missing:
        raise DocumentValidationError(
            f"Input data is missing required columns: {', '.join(sorted(missing))}"
        )
    normalised["document_chembl_id"] = normalised["document_chembl_id"].astype(str).str.strip().replace(["None", "nan", "NaN", "none", "NULL", "null"], pd.NA)
    normalised["doi"] = normalised["doi"].astype(str).str.strip().replace(["None", "nan", "NaN", "none", "NULL", "null"], pd.NA)
    normalised["title"] = normalised["title"].astype(str).str.strip().replace(["None", "nan", "NaN", "none", "NULL", "null"], pd.NA)
    
    # Обрабатываем дополнительные поля из исходного CSV, если они присутствуют
    # Маппинг старых имен колонок на новые
    if "classification" in normalised.columns:
        normalised["document_classification"] = pd.to_numeric(normalised["classification"], errors="coerce")
    
    # Маппинг pubmed_id на document_pubmed_id для совместимости с ETL
    if "pubmed_id" in normalised.columns and "document_pubmed_id" not in normalised.columns:
        normalised["document_pubmed_id"] = normalised["pubmed_id"]
    
    if "document_contains_external_links" in normalised.columns:
        normalised["referenses_on_previous_experiments"] = normalised["document_contains_external_links"].astype('boolean')
    
    if "is_experimental_doc" in normalised.columns:
        normalised["original_experimental_document"] = normalised["is_experimental_doc"].astype('boolean')
    
    # Также обрабатываем поля, если они уже имеют правильные имена
    if "document_classification" in normalised.columns:
        normalised["document_classification"] = pd.to_numeric(normalised["document_classification"], errors='coerce')
    
    if "referenses_on_previous_experiments" in normalised.columns:
        normalised["referenses_on_previous_experiments"] = normalised["referenses_on_previous_experiments"].astype('boolean')
    
    if "original_experimental_document" in normalised.columns:
        normalised["original_experimental_document"] = normalised["original_experimental_document"].astype('boolean')
    
    normalised = normalised.sort_values("document_chembl_id").reset_index(drop=True)
    return normalised


def _determine_publication_date(row: pd.Series) -> str:
    """
    Определяет дату публикации на основе полей PubMed.
    
    Логика определения:
    1. Если заданы pubmed_year_completed, pubmed_month_completed, pubmed_day_completed - 
       используется YYYY-MM-DD из этих полей
    2. Если эти значения не заданы - проверяются pubmed_year_revised, pubmed_month_revised, pubmed_day_revised
    3. Если и эти значения пусты - проверяется pubmed_year_completed и используется YYYY-01-01
    4. В противном случае проверяется pubmed_year_revised и используется YYYY-01-01
    5. В противном случае 0000-01-01
    
    Args:
        row: Строка DataFrame с данными документа
        
    Returns:
        str: Дата в формате YYYY-MM-DD
    """
    def _is_valid_value(value) -> bool:
        """Проверяет, что значение не пустое и не NaN."""
        return pd.notna(value) and str(value).strip() != ""
    
    def _format_date(year: str | int, month: str | int = "01", day: str | int = "01") -> str:
        """Форматирует дату в YYYY-MM-DD."""
        try:
            year_str = str(int(float(year))).zfill(4)
            month_str = str(int(float(month))).zfill(2)
            day_str = str(int(float(day))).zfill(2)
            return f"{year_str}-{month_str}-{day_str}"
        except (ValueError, TypeError):
            return "0000-01-01"
    
    # Проверяем pubmed_year_completed, pubmed_month_completed, pubmed_day_completed
    year_completed = row.get("pubmed_year_completed")
    month_completed = row.get("pubmed_month_completed")
    day_completed = row.get("pubmed_day_completed")
    
    if (_is_valid_value(year_completed) and 
        _is_valid_value(month_completed) and 
        _is_valid_value(day_completed)):
        return _format_date(year_completed, month_completed, day_completed)
    
    # Проверяем pubmed_year_revised, pubmed_month_revised, pubmed_day_revised
    year_revised = row.get("pubmed_year_revised")
    month_revised = row.get("pubmed_month_revised")
    day_revised = row.get("pubmed_day_revised")
    
    if (_is_valid_value(year_revised) and 
        _is_valid_value(month_revised) and 
        _is_valid_value(day_revised)):
        return _format_date(year_revised, month_revised, day_revised)
    
    # Проверяем только pubmed_year_completed
    if _is_valid_value(year_completed):
        return _format_date(year_completed)
    
    # Проверяем только pubmed_year_revised
    if _is_valid_value(year_revised):
        return _format_date(year_revised)
    
    # По умолчанию
    return "0000-01-01"


def _add_publication_date_column(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Добавляет колонку publication_date в DataFrame на основе полей PubMed.
    
    Args:
        frame: DataFrame с данными документов
        
    Returns:
        pd.DataFrame: DataFrame с добавленной колонкой publication_date
    """
    frame = frame.copy()
    
    # Применяем функцию определения даты публикации к каждой строке
    frame["publication_date"] = frame.apply(_determine_publication_date, axis=1)
    
    return frame


def _determine_document_sortorder(row: pd.Series) -> str:
    """
    Определяет порядок сортировки документов на основе pubmed_issn, publication_date и index.
    
    Логика определения:
    1. Каждый параметр приводится к строковому типу
    2. index дополняется символом "0" до общей длины в 6 символов
    3. Полученные строки склеиваются используя ":" как разделитель
    
    Args:
        row: Строка DataFrame с данными документа
        
    Returns:
        str: Строка для сортировки в формате "pubmed_issn:publication_date:index"
    """
    def _safe_str(value) -> str:
        """Безопасно преобразует значение в строку."""
        if pd.isna(value) or value is None:
            return ""
        return str(value).strip()
    
    def _pad_index(index_str: str) -> str:
        """Дополняет index нулями до 6 символов."""
        if not index_str:
            return "000000"
        # Преобразуем в int, если возможно, чтобы убрать .0 у float значений
        try:
            int_value = int(float(index_str))
            return str(int_value).zfill(6)
        except (ValueError, TypeError):
            return index_str.zfill(6)
    
    # Получаем значения и преобразуем в строки
    pubmed_issn = _safe_str(row.get("pubmed_issn", ""))
    publication_date = _safe_str(row.get("publication_date", ""))
    index = _safe_str(row.get("index", ""))
    
    # Дополняем index до 6 символов
    padded_index = _pad_index(index)
    
    # Склеиваем строки с разделителем ":"
    sortorder = f"{pubmed_issn}:{publication_date}:{padded_index}"
    
    return sortorder


def _add_document_sortorder_column(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Добавляет колонку document_sortorder в DataFrame на основе pubmed_issn, publication_date и index.
    
    Args:
        frame: DataFrame с данными документов
        
    Returns:
        pd.DataFrame: DataFrame с добавленной колонкой document_sortorder
    """
    frame = frame.copy()
    
    # Применяем функцию определения порядка сортировки к каждой строке
    frame["document_sortorder"] = frame.apply(_determine_document_sortorder, axis=1)
    
    return frame


def _initialize_all_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Initialize all possible output columns with default values."""
    
    # Define all possible columns that should exist in the output
    all_columns = {
        # Original ChEMBL fields
        "document_chembl_id", "title", "doi", "document_pubmed_id", "chembl_doc_type", "journal", "year",
        # Legacy ChEMBL fields
        "abstract", "pubmed_authors", "document_classification", "referenses_on_previous_experiments",
        "first_page", "original_experimental_document", "issue", "last_page", "month", "volume",
        # Enriched fields from external sources
        # source column removed - not needed in final output
        # ChEMBL-specific fields
        "chembl_title", "chembl_doi", "chembl_pmid", "chembl_journal", 
        "chembl_year", "chembl_volume", "chembl_issue",
        # Crossref-specific fields
        "crossref_doi", "crossref_title", "crossref_doc_type", "crossref_subject", "crossref_error",
        # OpenAlex-specific fields
        "openalex_doi", "openalex_title", "openalex_doc_type", 
        "openalex_crossref_doc_type", "openalex_year", "openalex_error",
        # PubMed-specific fields
        "pubmed_pmid", "pubmed_doi", "pubmed_title", "pubmed_abstract",
        "pubmed_journal", "pubmed_volume", "pubmed_issue", "pubmed_first_page", 
        "pubmed_last_page", "pubmed_doc_type", "pubmed_mesh_descriptors", 
        "pubmed_mesh_qualifiers", "pubmed_chemical_list", "pubmed_year_completed",
        "pubmed_month_completed", "pubmed_day_completed", "pubmed_year_revised",
        "pubmed_month_revised", "pubmed_day_revised", "pubmed_issn", "pubmed_error",
        # Semantic Scholar-specific fields
        "semantic_scholar_pmid", "semantic_scholar_doi", "semantic_scholar_semantic_scholar_id",
        "semantic_scholar_title", "semantic_scholar_doc_type", "semantic_scholar_journal", 
        "semantic_scholar_external_ids", "semantic_scholar_abstract", "semantic_scholar_issn",
        "semantic_scholar_authors", "semantic_scholar_error",
        # Common fields
        "doi_key", "index",
        # Citation field
        "document_citation",
        # Publication date field
        "publication_date",
        # Document sort order field
        "document_sortorder"
    }
    
    # Add missing columns with default values
    for column in all_columns:
        if column not in frame.columns:
            if column == "index":
                # Для колонки index используем порядковые номера строк
                frame[column] = range(len(frame))
            else:
                frame[column] = None
    
    # Ensure chembl_doc_type has a default value for all rows
    if "chembl_doc_type" in frame.columns:
        frame["chembl_doc_type"] = frame["chembl_doc_type"].fillna("PUBLICATION")
    
    # Ensure index column has proper values (replace None values with row indices)
    if "index" in frame.columns:
        # Заменяем None значения на реальные индексы строк
        frame["index"] = frame["index"].fillna(pd.Series(range(len(frame)), index=frame.index))
    
    return frame


def _calculate_checksum(file_path: Path) -> str:
    """Calculate SHA256 checksum of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def _add_tracking_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Add tracking fields: extracted_at, hash_business_key, hash_row."""
    import hashlib
    import json
    from datetime import datetime
    
    logger.info("Adding tracking fields: extracted_at, hash_business_key, hash_row")
    
    # Add extracted_at timestamp
    now_iso = datetime.utcnow().isoformat() + "Z"
    if "extracted_at" not in df.columns:
        df["extracted_at"] = now_iso
    else:
        df["extracted_at"] = df["extracted_at"].fillna(now_iso)
    
    # Add hash_business_key based on document_chembl_id
    if "document_chembl_id" in df.columns:
        df["hash_business_key"] = df["document_chembl_id"].apply(
            lambda x: hashlib.sha256(str(x).encode()).hexdigest()[:16] if pd.notna(x) else "unknown"
        )
    else:
        df["hash_business_key"] = "unknown"
    
    # Add hash_row for entire row deduplication
    df["hash_row"] = df.apply(
        lambda row: hashlib.sha256(
            json.dumps({k: v for k, v in row.to_dict().items() if not k.startswith("hash_")}, 
                      sort_keys=True, default=str).encode()
        ).hexdigest()[:16], 
        axis=1
    )
    
    logger.info("Tracking fields added successfully")
    return df


def run_document_etl(config: DocumentConfig, frame: pd.DataFrame) -> DocumentETLResult:
    """Execute the document ETL pipeline returning enriched artefacts."""

    normalised = _normalise_columns(frame)

    if config.runtime.limit is not None:
        normalised = normalised.head(config.runtime.limit)

    duplicates = normalised["document_chembl_id"].duplicated()
    if bool(duplicates.any()):
        raise DocumentQCError("Duplicate document_chembl_id values detected")

    # Use streaming batch processing
    batch_size = getattr(config.runtime, "batch_size", 100)
    total_rows = 0
    document_batches = []
    enabled_sources = config.enabled_sources()
    
    logger.info(f"Using streaming batch processing with batch_size={batch_size}")
    logger.info(f"Processing {len(normalised)} documents in batches")
    
    # Split documents into batches for streaming processing
    for batch_index, batch_frame in enumerate(_batch_dataframe(normalised, batch_size)):
        logger.info(f"Processing batch {batch_index + 1}: {len(batch_frame)} documents")
        
        # Initialize all possible columns with default values for this batch
        enriched_batch = _initialize_all_columns(batch_frame.copy())
        
        # Process each source for this batch
        for source in enabled_sources:
            try:
                logger.info(f"Extracting data from {source} for batch {batch_index + 1}...")
                client = _create_api_client(source, config)
                
                # Для источников с высоким rate limiting добавляем дополнительную задержку
                if source in ["semantic_scholar", "openalex"]:
                    import os
                    import time
                    
                    if source == "semantic_scholar":
                        # Проверяем, есть ли API ключ для Semantic Scholar
                        api_key = os.environ.get('SEMANTIC_SCHOLAR_API_KEY')
                        if api_key:
                            logger.info(f"Using Semantic Scholar with API key: {api_key[:10]}...")
                            logger.info("Semantic Scholar: Rate limiting controlled by configuration...")
                        else:
                            logger.info(f"Using conservative rate limiting for {source} (no API key)")
                            # Semantic Scholar имеет очень строгие ограничения без API ключа
                            logger.info("=" * 80)
                            logger.info("SEMANTIC SCHOLAR RATE LIMITING INFO:")
                            logger.info("Semantic Scholar API has very strict rate limits without an API key.")
                            logger.info("Current limit: 1 request per minute (controlled by rate limiter)")
                            logger.info("To get higher limits, apply for an API key at:")
                            logger.info("https://www.semanticscholar.org/product/api#api-key-form")
                            logger.info("=" * 80)
                            # Убираем избыточную задержку - rate limiter уже контролирует это
                            logger.info("Semantic Scholar: Rate limiting controlled by configuration...")
                    else:
                        logger.info(f"Using conservative rate limiting for {source}")
                        time.sleep(2)  # Дополнительная задержка для OpenAlex
                
                # Use batch processing where available
                enriched_batch = _extract_data_from_source_batch(source, client, enriched_batch, config)
                
                # Log success statistics for this batch
                if source == "chembl":
                    success_count = enriched_batch["chembl_title"].notna().sum()
                elif source == "crossref":
                    # Count successful records (either with title or with graceful degradation)
                    success_count = (enriched_batch["crossref_title"].notna() | 
                                   enriched_batch["crossref_error"].notna()).sum()
                elif source == "openalex":
                    success_count = enriched_batch["openalex_title"].notna().sum()
                elif source == "pubmed":
                    success_count = enriched_batch["pubmed_pmid"].notna().sum()
                elif source == "semantic_scholar":
                    success_count = enriched_batch["semantic_scholar_pmid"].notna().sum()
                else:
                    success_count = 0
                    
                logger.info(f"Successfully extracted data from {source} for batch {batch_index + 1}: "
                           f"{success_count}/{len(enriched_batch)} records")
                          
                # Для источников с высоким rate limiting добавляем задержку после завершения
                if source in ["semantic_scholar", "openalex"]:
                    logger.info("Rate limiting controlled by configuration...")
                    import time
                    if source == "semantic_scholar":
                        # Убираем избыточную задержку - rate limiter уже контролирует это
                        logger.info("Semantic Scholar: Rate limiting handled by configuration.")
                    else:
                        time.sleep(5)  # Задержка для OpenAlex
                    
            except Exception as exc:
                logger.warning(f"Failed to extract data from {source} for batch {batch_index + 1}: {exc}")
                logger.warning(f"Error type: {type(exc).__name__}")
                
                # Специальная информация для Semantic Scholar
                if source == "semantic_scholar":
                    logger.warning("=" * 80)
                    logger.warning("SEMANTIC SCHOLAR ERROR - RECOMMENDATIONS:")
                    logger.warning("1. Consider getting an API key for higher rate limits:")
                    logger.warning("   https://www.semanticscholar.org/product/api#api-key-form")
                    logger.warning("2. Or disable Semantic Scholar in your config file:")
                    logger.warning("   sources.semantic_scholar.enabled: false")
                    logger.warning("3. The pipeline will continue with other sources.")
                    logger.warning("=" * 80)
                
                # Continue with other sources even if one fails
        
        total_rows += len(enriched_batch)
        document_batches.append(enriched_batch)
        
        logger.info(f"Completed batch {batch_index + 1}: {len(enriched_batch)} documents, total={total_rows}")
        
        # Check limit
        if config.runtime.limit is not None and total_rows >= config.runtime.limit:
            logger.info(f"Reached global limit of {config.runtime.limit} documents")
            break
    
    if not document_batches:
        logger.info("No documents processed in streaming mode")
        enriched_frame = _initialize_all_columns(normalised.copy())
    else:
        enriched_frame = pd.concat(document_batches, ignore_index=True)
        if config.runtime.limit is not None and len(enriched_frame) > config.runtime.limit:
            enriched_frame = enriched_frame.head(config.runtime.limit)
    
    logger.info(f"Successfully processed {len(enriched_frame)} documents in {len(document_batches)} batches")

    # Добавляем колонку publication_date на основе полей PubMed
    logger.info("Добавляем колонку publication_date...")
    enriched_frame = _add_publication_date_column(enriched_frame)
    
    # Добавляем колонку document_sortorder на основе pubmed_issn, publication_date и index
    logger.info("Добавляем колонку document_sortorder...")
    enriched_frame = _add_document_sortorder_column(enriched_frame)
    
    # Добавляем технические поля для отслеживания
    logger.info("Добавляем технические поля отслеживания...")
    enriched_frame = _add_tracking_fields(enriched_frame)
    
    # Calculate QC metrics
    qc_metrics = [
        {"metric": "row_count", "value": int(len(enriched_frame))},
        {"metric": "enabled_sources", "value": len(enabled_sources)},
    ]
    
    # Add source-specific metrics based on non-null fields
    for source in enabled_sources:
        if source == "chembl":
            source_data_count = len(enriched_frame[enriched_frame["document_chembl_id"].notna()])
        elif source == "openalex":
            source_data_count = len(enriched_frame[enriched_frame["openalex_title"].notna()])
        elif source == "crossref":
            # Count records with either title or graceful degradation
            source_data_count = len(enriched_frame[
                enriched_frame["crossref_title"].notna() | 
                enriched_frame["crossref_error"].notna()
            ])
        elif source == "pubmed":
            if "pubmed_pmid" in enriched_frame.columns:
                source_data_count = len(enriched_frame[enriched_frame["pubmed_pmid"].notna()])
            else:
                source_data_count = 0
        elif source == "semantic_scholar":
            if "semantic_scholar_pmid" in enriched_frame.columns:
                source_data_count = len(
                    enriched_frame[enriched_frame["semantic_scholar_pmid"].notna()]
                )
            else:
                source_data_count = 0
        else:
            source_data_count = 0
        qc_metrics.append({"metric": f"{source}_records", "value": source_data_count})
    
    qc = pd.DataFrame(qc_metrics)
    # Унифицируем базовые QC метрики
    try:
        from library.etl.qc_common import ensure_common_qc
        qc = ensure_common_qc(enriched_frame, qc, module_name="documents")
    except Exception as exc:
        import logging as _logging
        _logging.getLogger(__name__).warning(f"Failed to normalize QC metrics for documents: {exc}")

    # Выполняем корреляционный анализ если включен в конфигурации
    correlation_analysis = None
    correlation_reports = None
    correlation_insights = None
    
    # Проверяем, включен ли корреляционный анализ в конфигурации
    if (hasattr(config, 'postprocess') and 
        hasattr(config.postprocess, 'correlation') and 
        config.postprocess.correlation.enabled):
        try:
            logger.info("Выполняем корреляционный анализ документов...")
            
            # Подготавливаем данные для корреляционного анализа
            analysis_df = prepare_data_for_correlation_analysis(
                enriched_frame, 
                data_type="documents", 
                logger=logger
            )
            
            # Выполняем корреляционный анализ
            correlation_analysis = build_enhanced_correlation_analysis(analysis_df, logger)
            correlation_reports = build_enhanced_correlation_reports(analysis_df, logger)
            correlation_insights = build_correlation_insights(analysis_df, logger)
            
            logger.info(f"Корреляционный анализ завершен. Найдено {len(correlation_insights)} инсайтов.")
            
        except Exception as exc:
            logger.warning(f"Ошибка при выполнении корреляционного анализа: {exc}")
            logger.warning(f"Тип ошибки: {type(exc).__name__}")
            # Продолжаем без корреляционного анализа

    # Создаем метаданные
    meta = {
        "pipeline_version": "1.0.0",
        "row_count": len(enriched_frame),
        "enabled_sources": enabled_sources,
        "extraction_parameters": {
            "total_documents": len(enriched_frame),
            "sources_processed": len(enabled_sources),
            "correlation_analysis_enabled": correlation_analysis is not None,
            "correlation_insights_count": len(correlation_insights) if correlation_insights else 0
        }
    }
    
    # Добавляем статистику по источникам
    for source in enabled_sources:
        if source == "chembl":
            source_data_count = len(enriched_frame[enriched_frame["document_chembl_id"].notna()])
        elif source == "openalex":
            source_data_count = len(enriched_frame[enriched_frame["openalex_title"].notna()])
        elif source == "crossref":
            source_data_count = len(enriched_frame[
                enriched_frame["crossref_title"].notna() | 
                enriched_frame["crossref_error"].notna()
            ])
        elif source == "pubmed":
            if "pubmed_pmid" in enriched_frame.columns:
                source_data_count = len(enriched_frame[enriched_frame["pubmed_pmid"].notna()])
            else:
                source_data_count = 0
        elif source == "semantic_scholar":
            if "semantic_scholar_pmid" in enriched_frame.columns:
                source_data_count = len(
                    enriched_frame[enriched_frame["semantic_scholar_pmid"].notna()]
                )
            else:
                source_data_count = 0
        else:
            source_data_count = 0
        
        meta["extraction_parameters"][f"{source}_records"] = source_data_count

    return DocumentETLResult(
        documents=enriched_frame, 
        qc=qc,
        meta=meta,
        correlation_analysis=correlation_analysis,
        correlation_reports=correlation_reports,
        correlation_insights=correlation_insights
    )


def write_document_outputs(
    result: DocumentETLResult, output_dir: Path, date_tag: str, config: DocumentConfig | None = None
) -> dict[str, Path]:
    """Persist ETL artefacts to disk and return the generated paths."""

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:  # pragma: no cover - filesystem permission issues
        raise DocumentIOError(f"Failed to create output directory: {exc}") from exc

    documents_path = output_dir / f"documents_{date_tag}.csv"
    qc_path = output_dir / f"documents_{date_tag}_qc.csv"
    meta_path = output_dir / f"documents_{date_tag}_meta.yaml"

    try:
        # Нормализуем колонки с названиями журналов (если включено)
        if config is not None and config.postprocess.journal_normalization.enabled:
            documents_normalized = normalize_journal_columns(result.documents)
        else:
            documents_normalized = result.documents
        
        # Валидируем данные из различных источников
        from library.tools.data_validator import validate_all_fields
        documents_validated = validate_all_fields(documents_normalized)
        
        # Добавляем колонку с литературными ссылками после валидации (если включено)
        if config is not None and config.postprocess.citation_formatting.enabled:
            # Получаем маппинг колонок из конфигурации, если он есть
            column_mapping = getattr(config.postprocess.citation_formatting, 'columns', None)
            documents_with_citations = add_citation_column(documents_validated, column_mapping)
        else:
            documents_with_citations = documents_validated
        
        # Используем готовую функцию детерминистического сохранения
        if config is not None:
            from library.etl.load import write_deterministic_csv
            write_deterministic_csv(
                documents_with_citations,
                documents_path,
                determinism=config.determinism,
                output=None  # Используем fallback настройки
            )
        else:
            # Fallback для случаев, когда конфигурация не передана
            documents_with_citations.to_csv(documents_path, index=False)
        
        # Сохраняем QC данные также с детерминистическим порядком
        if config is not None:
            write_deterministic_csv(
                result.qc,
                qc_path,
                determinism=config.determinism,
                output=None  # Используем fallback настройки
            )
        else:
            result.qc.to_csv(qc_path, index=False)
        
        # Save metadata
        import yaml
        with open(meta_path, 'w', encoding='utf-8') as f:
            yaml.dump(result.meta, f, default_flow_style=False, allow_unicode=True)
        
        # Add file checksums to metadata
        result.meta["file_checksums"] = {
            "csv": _calculate_checksum(documents_path),
            "qc": _calculate_checksum(qc_path)
        }
        
        # Update metadata file with checksums
        with open(meta_path, 'w', encoding='utf-8') as f:
            yaml.dump(result.meta, f, default_flow_style=False, allow_unicode=True)
            
    except OSError as exc:  # pragma: no cover - filesystem permission issues
        raise DocumentIOError(f"Failed to write outputs: {exc}") from exc

    outputs = {"documents": documents_path, "qc": qc_path, "meta": meta_path}

    # Сохраняем корреляционные отчеты если они есть
    if result.correlation_reports:
        try:
            correlation_dir = output_dir / f"documents_correlation_report_{date_tag}"
            correlation_dir.mkdir(exist_ok=True)
            
            # Сохраняем каждый тип корреляционного отчета
            for report_name, report_df in result.correlation_reports.items():
                if not report_df.empty:
                    report_path = correlation_dir / f"{report_name}.csv"
                    report_df.to_csv(report_path, index=True)
                    outputs[f"correlation_{report_name}"] = report_path
            
            # Сохраняем инсайты как JSON
            if result.correlation_insights:
                import json
                insights_path = correlation_dir / "correlation_insights.json"
                with open(insights_path, 'w', encoding='utf-8') as f:
                    json.dump(result.correlation_insights, f, ensure_ascii=False, indent=2)
                outputs["correlation_insights"] = insights_path
            
            logger.info(f"Корреляционные отчеты сохранены в: {correlation_dir}")
            
        except Exception as exc:
            logger.warning(f"Ошибка при сохранении корреляционных отчетов: {exc}")
            logger.warning(f"Тип ошибки: {type(exc).__name__}")

    return outputs


def _collect_identifiers(frame: pd.DataFrame, source: str) -> list[tuple[str, str]]:
    """Collect identifiers for batch processing.
    
    Returns: List of (identifier_type, identifier_value) tuples
    """
    identifiers = []
    
    for _, row in frame.iterrows():
        if source == "chembl":
            if pd.notna(row.get("document_chembl_id")):
                identifiers.append(("document_chembl_id", str(row["document_chembl_id"]).strip()))
        elif source == "crossref":
            if pd.notna(row.get("doi")):
                identifiers.append(("doi", str(row["doi"]).strip()))
            elif pd.notna(row.get("document_pubmed_id")):
                identifiers.append(("pmid", str(row["document_pubmed_id"]).strip()))
        elif source == "openalex":
            if pd.notna(row.get("doi")):
                identifiers.append(("doi", str(row["doi"]).strip()))
            elif pd.notna(row.get("document_pubmed_id")):
                identifiers.append(("pmid", str(row["document_pubmed_id"]).strip()))
        elif source == "pubmed":
            if pd.notna(row.get("document_pubmed_id")):
                identifiers.append(("pmid", str(row["document_pubmed_id"]).strip()))
        elif source == "semantic_scholar":
            if pd.notna(row.get("document_pubmed_id")):
                identifiers.append(("pmid", str(row["document_pubmed_id"]).strip()))
    
    return identifiers


def _chunk_list(items: list, chunk_size: int) -> Generator[list, None, None]:
    """Split list into chunks."""
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def _batch_dataframe(df: pd.DataFrame, batch_size: int) -> Generator[pd.DataFrame, None, None]:
    """Split DataFrame into batches."""
    for i in range(0, len(df), batch_size):
        yield df.iloc[i:i + batch_size].copy()


def _merge_batch_results(
    original_frame: pd.DataFrame,
    batch_data: dict[str, dict[str, Any]],
    source: str
) -> pd.DataFrame:
    """Merge batch API results with original frame."""
    enriched_data = []
    
    for _, row in original_frame.iterrows():
        row_data = row.to_dict()
        
        # Find matching identifier for this source
        identifier = None
        if source == "chembl" and pd.notna(row.get("document_chembl_id")):
            identifier = str(row["document_chembl_id"]).strip()
        elif source in ["crossref", "openalex"]:
            if pd.notna(row.get("doi")):
                identifier = str(row["doi"]).strip()
            elif pd.notna(row.get("document_pubmed_id")):
                identifier = str(row["document_pubmed_id"]).strip()
        elif source in ["pubmed", "semantic_scholar"]:
            if pd.notna(row.get("document_pubmed_id")):
                identifier = str(row["document_pubmed_id"]).strip()
        
        # Merge batch data if found
        if identifier and identifier in batch_data:
            batch_result = batch_data[identifier]
            # Remove source field to avoid overwriting
            batch_result.pop("source", None)
            # Always preserve error fields, update other fields only if not None
            for key, value in batch_result.items():
                if key.endswith("_error") or value is not None:
                    row_data[key] = value
        
        enriched_data.append(row_data)
    
    return pd.DataFrame(enriched_data)


__all__ = [
    "DocumentETLResult",
    "DocumentHTTPError",
    "DocumentIOError",
    "DocumentPipelineError",
    "DocumentQCError",
    "DocumentValidationError",
    "_add_document_sortorder_column",
    "_add_publication_date_column",
    "_add_tracking_fields",
    "_calculate_checksum",
    "_create_api_client",
    "_determine_document_sortorder",
    "_determine_publication_date",
    "_extract_data_from_source",
    "read_document_input",
    "run_document_etl",
    "write_document_outputs",
]
class DocumentPipeline(PipelineBase[DocumentConfig]):
    """Document ETL pipeline using unified PipelineBase."""

    def __init__(self, config: DocumentConfig) -> None:
        """Initialize document pipeline with configuration."""
        super().__init__(config)
        self.validator = DocumentValidator(config.model_dump() if hasattr(config, "model_dump") else config if isinstance(config, dict) else {})
        self.normalizer = DocumentNormalizer(config.model_dump() if hasattr(config, "model_dump") else config if isinstance(config, dict) else {})
        self.quality_filter = DocumentQualityFilter(config.model_dump() if hasattr(config, "model_dump") else config if isinstance(config, dict) else {})
        self.diagnostics = DocumentDiagnostics()

    def _setup_clients(self) -> None:
        """Initialize HTTP clients for document sources."""
        self.clients = {}

        # ChEMBL client
        if "chembl" in self.config.sources and self.config.sources["chembl"].enabled:
            self.clients["chembl"] = self._create_chembl_client()

        # Crossref client
        if "crossref" in self.config.sources and self.config.sources["crossref"].enabled:
            self.clients["crossref"] = self._create_crossref_client()

        # OpenAlex client
        if "openalex" in self.config.sources and self.config.sources["openalex"].enabled:
            self.clients["openalex"] = self._create_openalex_client()

        # PubMed client
        if "pubmed" in self.config.sources and self.config.sources["pubmed"].enabled:
            self.clients["pubmed"] = self._create_pubmed_client()

        # Semantic Scholar client
        if "semantic_scholar" in self.config.sources and self.config.sources["semantic_scholar"].enabled:
            self.clients["semantic_scholar"] = self._create_semantic_scholar_client()

    def _create_chembl_client(self) -> ChEMBLClient:
        """Create ChEMBL client."""
        from library.settings import APIClientConfig, RateLimitSettings, RetrySettings

        source_config = self.config.sources["chembl"]
        timeout = source_config.http.timeout_sec or self.config.http.global_.timeout_sec
        timeout = max(timeout, 60.0)  # At least 60 seconds for ChEMBL

        headers = self._get_headers("chembl")
        headers.update(self.config.http.global_.headers)
        headers.update(source_config.http.headers)

        # Process secret placeholders
        processed_headers = self._process_headers(headers)

        client_config = APIClientConfig(
            name="chembl",
            base_url=source_config.http.base_url or "https://www.ebi.ac.uk/chembl/api/data",
            timeout_sec=timeout,
            retries=RetrySettings(
                total=source_config.http.retries.get("total", 3),
                backoff_multiplier=source_config.http.retries.get("backoff_multiplier", 2.0),
                backoff_max=source_config.http.retries.get("backoff_max", 60.0),
            ),
            rate_limit=RateLimitSettings(
                max_calls=source_config.rate_limit.get("max_calls", 10),
                period=source_config.rate_limit.get("period", 1.0),
            ),
            headers=processed_headers,
            verify_ssl=getattr(source_config.http, "verify_ssl", None) or True,
            follow_redirects=getattr(source_config.http, "follow_redirects", None) or True,
        )

        return ChEMBLClient(client_config)

    def _create_crossref_client(self) -> CrossrefClient:
        """Create Crossref client."""
        from library.settings import APIClientConfig, RateLimitSettings, RetrySettings

        source_config = self.config.sources["crossref"]
        timeout = source_config.http.timeout_sec or self.config.http.global_.timeout_sec

        # Получаем mailto из конфигурации
        mailto = getattr(source_config, "mailto", None) or "821311@gmail.com"

        headers = self._get_headers("crossref")
        headers.update(self.config.http.global_.headers)
        headers.update(source_config.http.headers)

        processed_headers = self._process_headers(headers)

        # Получаем timeout настройки
        timeout_connect = getattr(source_config.http, "timeout_connect", None) or 10.0
        timeout_read = getattr(source_config.http, "timeout_read", None) or 30.0

        # Получаем rate limit с burst поддержкой
        rate_limit_config = source_config.rate_limit or {}
        max_calls = rate_limit_config.get("max_calls", 2)  # Снижено с 10 до 2
        period = rate_limit_config.get("period", 1.0)
        burst = rate_limit_config.get("burst", 5)  # НОВОЕ - burst поддержка

        client_config = APIClientConfig(
            name="crossref",
            base_url=source_config.http.base_url or "https://api.crossref.org",
            timeout_sec=timeout,
            timeout_connect=timeout_connect,  # НОВОЕ
            timeout_read=timeout_read,  # НОВОЕ
            retries=RetrySettings(
                total=source_config.http.retries.get("total", 3),
                backoff_multiplier=source_config.http.retries.get("backoff_multiplier", 2.0),
                backoff_max=source_config.http.retries.get("backoff_max", 60.0),
            ),
            rate_limit=RateLimitSettings(
                max_calls=max_calls,
                period=period,
                burst=burst,  # НОВОЕ
            ),
            headers=processed_headers,
            verify_ssl=getattr(source_config.http, "verify_ssl", None) or True,
            follow_redirects=getattr(source_config.http, "follow_redirects", None) or True,
            mailto=mailto,  # НОВОЕ
        )

        return CrossrefClient(client_config)

    def _create_openalex_client(self) -> OpenAlexClient:
        """Create OpenAlex client."""
        from library.settings import APIClientConfig, RateLimitSettings, RetrySettings

        source_config = self.config.sources["openalex"]
        timeout = source_config.http.timeout_sec or self.config.http.global_.timeout_sec

        headers = self._get_headers("openalex")
        headers.update(self.config.http.global_.headers)
        headers.update(source_config.http.headers)

        processed_headers = self._process_headers(headers)

        client_config = APIClientConfig(
            name="openalex",
            base_url=source_config.http.base_url or "https://api.openalex.org",
            timeout_sec=timeout,
            retries=RetrySettings(
                total=source_config.http.retries.get("total", 3),
                backoff_multiplier=source_config.http.retries.get("backoff_multiplier", 2.0),
                backoff_max=source_config.http.retries.get("backoff_max", 60.0),
            ),
            rate_limit=RateLimitSettings(
                max_calls=source_config.rate_limit.get("max_calls", 10),
                period=source_config.rate_limit.get("period", 1.0),
            ),
            headers=processed_headers,
            verify_ssl=getattr(source_config.http, "verify_ssl", None) or True,
            follow_redirects=getattr(source_config.http, "follow_redirects", None) or True,
        )

        return OpenAlexClient(client_config)

    def _create_pubmed_client(self) -> PubMedClient:
        """Create PubMed client."""
        from library.settings import APIClientConfig, RateLimitSettings, RetrySettings

        source_config = self.config.sources["pubmed"]
        timeout = source_config.http.timeout_sec or self.config.http.global_.timeout_sec

        headers = self._get_headers("pubmed")
        headers.update(self.config.http.global_.headers)
        headers.update(source_config.http.headers)

        processed_headers = self._process_headers(headers)

        client_config = APIClientConfig(
            name="pubmed",
            base_url=source_config.http.base_url or "https://eutils.ncbi.nlm.nih.gov/entrez/eutils",
            timeout_sec=timeout,
            retries=RetrySettings(
                total=source_config.http.retries.get("total", 3),
                backoff_multiplier=source_config.http.retries.get("backoff_multiplier", 2.0),
                backoff_max=source_config.http.retries.get("backoff_max", 60.0),
            ),
            rate_limit=RateLimitSettings(
                max_calls=source_config.rate_limit.get("max_calls", 10),
                period=source_config.rate_limit.get("period", 1.0),
            ),
            headers=processed_headers,
            verify_ssl=getattr(source_config.http, "verify_ssl", None) or True,
            follow_redirects=getattr(source_config.http, "follow_redirects", None) or True,
        )

        return PubMedClient(client_config)

    def _create_semantic_scholar_client(self) -> SemanticScholarClient:
        """Create Semantic Scholar client."""
        from library.settings import APIClientConfig, RateLimitSettings, RetrySettings

        source_config = self.config.sources["semantic_scholar"]
        timeout = source_config.http.timeout_sec or self.config.http.global_.timeout_sec

        headers = self._get_headers("semantic_scholar")
        headers.update(self.config.http.global_.headers)
        headers.update(source_config.http.headers)

        processed_headers = self._process_headers(headers)

        client_config = APIClientConfig(
            name="semantic_scholar",
            base_url=source_config.http.base_url or "https://api.semanticscholar.org",
            timeout_sec=timeout,
            retries=RetrySettings(
                total=source_config.http.retries.get("total", 3),
                backoff_multiplier=source_config.http.retries.get("backoff_multiplier", 2.0),
                backoff_max=source_config.http.retries.get("backoff_max", 60.0),
            ),
            rate_limit=RateLimitSettings(
                max_calls=source_config.rate_limit.get("max_calls", 10),
                period=source_config.rate_limit.get("period", 1.0),
            ),
            headers=processed_headers,
            verify_ssl=getattr(source_config.http, "verify_ssl", None) or True,
            follow_redirects=getattr(source_config.http, "follow_redirects", None) or True,
        )

        return SemanticScholarClient(client_config)

    def _get_headers(self, source: str) -> dict[str, str]:
        """Get default headers for a source."""
        headers = {
            "Accept": "application/json",
            "User-Agent": "bioactivity-data-acquisition/0.1.0",
        }

        if source == "crossref":
            headers["Accept"] = "application/vnd.crossref.unixsd+xml"
        elif source == "pubmed":
            headers["Accept"] = "application/xml"

        return headers

    def _process_headers(self, headers: dict[str, str]) -> dict[str, str]:
        """Process headers with secret placeholders."""
        processed = {}
        for key, value in headers.items():
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                # Handle environment variable substitution
                env_var = value[2:-1]
                processed[key] = os.getenv(env_var, value)
            else:
                processed[key] = value
        return processed

    def extract(self, input_data: pd.DataFrame) -> pd.DataFrame:
        """Extract document data from multiple sources."""
        logger.info(f"Extracting document data for {len(input_data)} documents")

        # Normalize input columns first
        normalized_data = self._normalize_columns(input_data)

        # Apply limit if specified
        if self.config.runtime.limit is not None:
            normalized_data = normalized_data.head(self.config.runtime.limit)

        # Check for duplicates
        duplicates = normalized_data["document_chembl_id"].duplicated()
        if duplicates.any():
            raise ValueError("Duplicate document_chembl_id values detected")

        # Extract data from each enabled source
        extracted_data = normalized_data.copy()

        for source_name, client in self.clients.items():
            try:
                logger.info(f"Extracting data from {source_name}")
                source_data = self._extract_from_source(client, source_name, normalized_data)
                extracted_data = self._merge_source_data(extracted_data, source_data, source_name)

                # Логируем статистику извлечения
                if not source_data.empty:
                    logger.info(f"Successfully extracted {len(source_data)} records from {source_name}")
                else:
                    logger.warning(f"No data extracted from {source_name}")

            except Exception as e:
                logger.error(f"Failed to extract from {source_name}: {e}", exc_info=True)

                # Создаем DataFrame с ошибками для graceful degradation
                error_data = self._create_error_dataframe(normalized_data, source_name, str(e))

                # Простое объединение без сложной логики для ошибок
                try:
                    extracted_data = self._merge_source_data(extracted_data, error_data, source_name)
                except Exception as merge_error:
                    logger.error(f"Failed to merge error data from {source_name}: {merge_error}")
                    # Добавляем колонки с ошибками напрямую
                    error_column = f"{source_name}_error"
                    extracted_data[error_column] = str(e)

                if not self.config.runtime.allow_incomplete_sources:
                    raise

        logger.info(f"Extracted data for {len(extracted_data)} documents")

        # Генерируем диагностический отчет после extract
        try:
            logger.info("Generating diagnostic report for extracted data")
            diagnostic_report = self.diagnostics.analyze_dataframe(extracted_data)
            summary_report = self.diagnostics.generate_summary_report(extracted_data)

            # Логируем краткий отчет
            logger.info("=== EXTRACTION DIAGNOSTICS ===")
            logger.info(summary_report)

            # Сохраняем полный отчет
            report_path = self.diagnostics.save_report(diagnostic_report)
            logger.info(f"Full diagnostic report saved to {report_path}")

        except Exception as e:
            logger.warning(f"Failed to generate diagnostic report: {e}")

        return extracted_data

    def _normalize_columns(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Normalize input columns."""
        normalized = frame.copy()

        # Remove deprecated columns
        if "postcodes" in normalized.columns:
            normalized = normalized.drop(columns=["postcodes"])

        # Map column names to expected schema names
        column_mapping = {
            "DOI": "doi",  # Map DOI to doi
            "pubmed_id": "document_pubmed_id",
            "authors": "pubmed_authors",
            "classification": "document_classification",
            "document_contains_external_links": "referenses_on_previous_experiments",
            "is_experimental_doc": "original_experimental_document",
            "title": "chembl_title",
            "journal": "chembl_journal",
            "volume": "chembl_volume",
            "issue": "chembl_issue",
            "year": "chembl_year",
        }

        # Rename columns and remove old ones
        columns_to_drop = []
        for old_name, new_name in column_mapping.items():
            if old_name in normalized.columns:
                normalized[new_name] = normalized[old_name]
                columns_to_drop.append(old_name)

        # Drop old column names to avoid confusion
        normalized = normalized.drop(columns=columns_to_drop, errors="ignore")

        # Add diagnostic logging
        logger.info(f"Normalized columns: {list(normalized.columns)}")
        if "document_pubmed_id" in normalized.columns:
            pmid_count = normalized["document_pubmed_id"].notna().sum()
            logger.info(f"Records with PMID: {pmid_count}/{len(normalized)}")
        if "doi" in normalized.columns:
            doi_count = normalized["doi"].notna().sum()
            logger.info(f"Records with DOI: {doi_count}/{len(normalized)}")

        # Check required columns
        required_columns = {"document_chembl_id", "doi"}
        present = set(normalized.columns)
        missing = required_columns - present
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # Normalize document_chembl_id
        if "document_chembl_id" in normalized.columns:
            normalized["document_chembl_id"] = normalized["document_chembl_id"].astype(str).str.strip().replace(["None", "nan", "NaN", "none", "NULL", "null"], pd.NA)
            normalized = normalized[normalized["document_chembl_id"] != ""]

        return normalized.sort_values("document_chembl_id").reset_index(drop=True)

    def _extract_from_source(self, client: Any, source_name: str, data: pd.DataFrame) -> pd.DataFrame:
        """Extract data from a specific source."""
        from library.documents.extract import (
            extract_from_chembl,
            extract_from_crossref,
            extract_from_openalex,
            extract_from_pubmed,
            extract_from_semantic_scholar,
        )

        if source_name == "pubmed":
            # Извлекаем PMID из данных
            pmids = []
            if "document_pubmed_id" in data.columns:
                pmids = data["document_pubmed_id"].dropna().astype(str).unique().tolist()
            elif "pubmed_id" in data.columns:
                pmids = data["pubmed_id"].dropna().astype(str).unique().tolist()

            if pmids:
                batch_size = getattr(self.config.sources.get("pubmed", {}), "batch_size", 200)
                return extract_from_pubmed(client, pmids, batch_size)
            else:
                logger.warning("No PMIDs found for PubMed extraction")
                return pd.DataFrame()

        elif source_name == "crossref":
            # Извлекаем DOI из данных
            dois = []
            if "doi" in data.columns:
                dois = data["doi"].dropna().astype(str).unique().tolist()
            elif "DOI" in data.columns:
                dois = data["DOI"].dropna().astype(str).unique().tolist()

            if dois:
                batch_size = getattr(self.config.sources.get("crossref", {}), "batch_size", 100)
                return extract_from_crossref(client, dois, batch_size)
            else:
                logger.warning("No DOIs found for Crossref extraction")
                return pd.DataFrame()

        elif source_name == "openalex":
            # Извлекаем PMID для OpenAlex
            pmids = []
            if "document_pubmed_id" in data.columns:
                pmids = data["document_pubmed_id"].dropna().astype(str).unique().tolist()
            elif "pubmed_id" in data.columns:
                pmids = data["pubmed_id"].dropna().astype(str).unique().tolist()

            if pmids:
                batch_size = getattr(self.config.sources.get("openalex", {}), "batch_size", 50)
                return extract_from_openalex(client, pmids, batch_size)
            else:
                logger.warning("No PMIDs found for OpenAlex extraction")
                return pd.DataFrame()

        elif source_name == "semantic_scholar":
            # Извлекаем PMID и заголовки для Semantic Scholar
            pmids = []
            titles = {}

            if "document_pubmed_id" in data.columns:
                pmids = data["document_pubmed_id"].dropna().astype(str).unique().tolist()
                # Создаем маппинг PMID -> заголовок
                for _, row in data.iterrows():
                    pmid = str(row.get("document_pubmed_id", ""))
                    title = row.get("document_title", "") or row.get("title", "")
                    if pmid and title:
                        titles[pmid] = title
            elif "pubmed_id" in data.columns:
                pmids = data["pubmed_id"].dropna().astype(str).unique().tolist()
                # Создаем маппинг PMID -> заголовок
                for _, row in data.iterrows():
                    pmid = str(row.get("pubmed_id", ""))
                    title = row.get("document_title", "") or row.get("title", "")
                    if pmid and title:
                        titles[pmid] = title

            if pmids:
                batch_size = getattr(self.config.sources.get("semantic_scholar", {}), "batch_size", 100)
                return extract_from_semantic_scholar(client, pmids, batch_size, titles)
            else:
                logger.warning("No PMIDs found for Semantic Scholar extraction")
                return pd.DataFrame()

        elif source_name == "chembl":
            # Извлекаем ChEMBL ID
            chembl_ids = []
            if "document_chembl_id" in data.columns:
                chembl_ids = data["document_chembl_id"].dropna().astype(str).unique().tolist()

            if chembl_ids:
                batch_size = getattr(self.config.sources.get("chembl", {}), "batch_size", 100)
                return extract_from_chembl(client, chembl_ids, batch_size)
            else:
                logger.warning("No ChEMBL IDs found for ChEMBL extraction")
                return pd.DataFrame()

        else:
            logger.warning(f"Unknown source: {source_name}")
            return pd.DataFrame()

    def _merge_source_data(self, base_data: pd.DataFrame, source_data: pd.DataFrame, source_name: str) -> pd.DataFrame:
        """Merge data from a source into base data."""
        from library.documents.merge import merge_source_data

        if source_data.empty:
            logger.warning(f"No data to merge from {source_name}")
            return base_data

        # Определить ключ объединения
        if source_name in ["pubmed", "openalex", "semantic_scholar"]:
            join_key = "document_pubmed_id"
        elif source_name == "crossref":
            join_key = "doi"
        elif source_name == "chembl":
            join_key = "document_chembl_id"
        else:
            logger.warning(f"Unknown source {source_name}")
            return base_data

        # Объединить данные
        merged = merge_source_data(base_df=base_data, source_df=source_data, source_name=source_name, join_key=join_key)

        logger.info(f"Merged {len(source_data)} records from {source_name}")
        return merged

    def _create_error_dataframe(self, base_data: pd.DataFrame, source_name: str, error_message: str) -> pd.DataFrame:
        """Создать DataFrame с ошибками для graceful degradation."""
        error_column = f"{source_name}_error"

        # Создаем DataFrame с ошибками для всех записей
        error_data = base_data[["document_chembl_id"]].copy()
        error_data[error_column] = error_message

        # Добавляем пустые поля для источника
        source_fields = {
            "pubmed": ["PubMed.PMID", "PubMed.Error"],
            "crossref": ["crossref.DOI", "crossref.Error"],
            "openalex": ["OpenAlex.PMID", "OpenAlex.Error"],
            "semantic_scholar": ["scholar.PMID", "scholar.Error"],
            "chembl": ["ChEMBL.document_chembl_id", "ChEMBL.Error"],
        }

        if source_name in source_fields:
            for field in source_fields[source_name]:
                if field.endswith("Error"):
                    error_data[field] = error_message
                else:
                    error_data[field] = ""

        return error_data

    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Normalize document data."""
        from library.documents.merge import (
            add_document_sortorder,
            compute_publication_date,
            convert_data_types,
        )

        logger.info("Normalizing document data")

        # Apply document normalization
        normalized_data = self.normalizer.normalize_documents(raw_data)

        # Apply journal normalization
        normalized_data = normalize_journal_columns(normalized_data)

        # Add citation formatting
        normalized_data = add_citation_column(normalized_data)

        # Compute publication date from all sources
        normalized_data = compute_publication_date(normalized_data)

        # Add document sort order
        normalized_data = add_document_sortorder(normalized_data)

        # Convert data types to match schema
        normalized_data = convert_data_types(normalized_data)

        logger.info(f"Normalized {len(normalized_data)} documents")

        # Генерируем финальный диагностический отчет
        try:
            logger.info("Generating final diagnostic report")
            final_report = self.diagnostics.analyze_dataframe(normalized_data)
            final_summary = self.diagnostics.generate_summary_report(normalized_data)

            # Логируем финальный отчет
            logger.info("=== FINAL PIPELINE DIAGNOSTICS ===")
            logger.info(final_summary)

            # Сохраняем финальный отчет
            final_report_path = self.diagnostics.save_report(final_report, "final_diagnostics.json")
            logger.info(f"Final diagnostic report saved to {final_report_path}")

        except Exception as e:
            logger.warning(f"Failed to generate final diagnostic report: {e}")

        return normalized_data

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate document data."""
        logger.info("Validating document data")

        # Validate normalized data
        validated_data = self.validator.validate_normalized(data)

        logger.info(f"Validated {len(validated_data)} documents")
        return validated_data

    def filter_quality(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Filter documents by quality."""
        logger.info("Filtering documents by quality")

        # Apply quality filters
        accepted_data, rejected_data = self.quality_filter.apply_moderate_quality_filter(data)

        logger.info(f"Quality filtering: {len(accepted_data)} accepted, {len(rejected_data)} rejected")
        return accepted_data, rejected_data

    def _get_entity_type(self) -> str:
        """Получить тип сущности для пайплайна."""
        return "documents"

    def _create_qc_validator(self) -> Any:
        """Создать QC валидатор для пайплайна."""
        from library.common.qc_profiles import DocumentQCValidator, QCProfile

        # Создаем базовый QC профиль для документов
        qc_profile = QCProfile(name="document_qc", description="Quality control profile for documents", rules=[])

        return DocumentQCValidator(qc_profile)

    def _create_postprocessor(self) -> Any:
        """Создать постпроцессор для пайплайна."""
        from library.common.postprocess_base import DocumentPostprocessor

        return DocumentPostprocessor(self.config)

    def _create_etl_writer(self) -> Any:
        """Создать ETL writer для пайплайна."""
        from library.common.writer_base import create_etl_writer

        return create_etl_writer(self.config, "documents")
