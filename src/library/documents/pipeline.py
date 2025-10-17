"""Simplified document ETL pipeline orchestration."""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from library.clients.chembl import ChEMBLClient
from library.clients.crossref import CrossrefClient
from library.clients.openalex import OpenAlexClient
from library.clients.pubmed import PubMedClient
from library.clients.semantic_scholar import SemanticScholarClient
from library.config import APIClientConfig
from library.documents.config import DocumentConfig
from library.etl.enhanced_correlation import (
    build_correlation_insights,
    build_enhanced_correlation_analysis,
    build_enhanced_correlation_reports,
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
        "openalex": "https://api.openalex.org/works",
        "pubmed": "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/",
        "semantic_scholar": "https://api.semanticscholar.org/graph/v1/paper",
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
        "pubmed_pmid": None, "pubmed_doi": None, "pubmed_article_title": None, 
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
        "chembl_doc_type": None, "doi_key": None, "index": None
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
                    data = client.fetch_by_pmid(str(row["document_pubmed_id"]).strip())
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
    normalised["document_chembl_id"] = normalised["document_chembl_id"].astype(str).str.strip()
    normalised["doi"] = normalised["doi"].astype(str).str.strip()
    normalised["title"] = normalised["title"].astype(str).str.strip()
    
    # Обрабатываем дополнительные поля из исходного CSV, если они присутствуют
    # Маппинг старых имен колонок на новые
    if "classification" in normalised.columns:
        normalised["document_classification"] = pd.to_numeric(normalised["classification"], errors='coerce')
    
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
        "pubmed_pmid", "pubmed_doi", "pubmed_article_title", "pubmed_abstract",
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
            frame[column] = None
    
    # Ensure chembl_doc_type has a default value for all rows
    if "chembl_doc_type" in frame.columns:
        frame["chembl_doc_type"] = frame["chembl_doc_type"].fillna("PUBLICATION")
    
    return frame


def run_document_etl(config: DocumentConfig, frame: pd.DataFrame) -> DocumentETLResult:
    """Execute the document ETL pipeline returning enriched artefacts."""

    normalised = _normalise_columns(frame)

    if config.runtime.limit is not None:
        normalised = normalised.head(config.runtime.limit)

    duplicates = normalised["document_chembl_id"].duplicated()
    if bool(duplicates.any()):
        raise DocumentQCError("Duplicate document_chembl_id values detected")

    # Initialize all possible columns with default values
    enriched_frame = _initialize_all_columns(normalised.copy())
    enabled_sources = config.enabled_sources()
    
    for source in enabled_sources:
        try:
            logger.info(f"Extracting data from {source}...")
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
            
            enriched_frame = _extract_data_from_source(source, client, enriched_frame, config)
            
            # Log success statistics
            if source == "chembl":
                success_count = enriched_frame["chembl_title"].notna().sum()
            elif source == "crossref":
                # Count successful records (either with title or with graceful degradation)
                success_count = (enriched_frame["crossref_title"].notna() | 
                               enriched_frame["crossref_error"].notna()).sum()
            elif source == "openalex":
                success_count = enriched_frame["openalex_title"].notna().sum()
            elif source == "pubmed":
                success_count = enriched_frame["pubmed_pmid"].notna().sum()
            elif source == "semantic_scholar":
                success_count = enriched_frame["semantic_scholar_pmid"].notna().sum()
            else:
                success_count = 0
                
            logger.info(f"Successfully extracted data from {source}: "
                       f"{success_count}/{len(enriched_frame)} records")
                  
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
            logger.warning(f"Failed to extract data from {source}: {exc}")
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

    # Добавляем колонку publication_date на основе полей PubMed
    logger.info("Добавляем колонку publication_date...")
    enriched_frame = _add_publication_date_column(enriched_frame)
    
    # Добавляем колонку document_sortorder на основе pubmed_issn, publication_date и index
    logger.info("Добавляем колонку document_sortorder...")
    enriched_frame = _add_document_sortorder_column(enriched_frame)
    
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
            
            # Создаем временный DataFrame для анализа (только числовые и категориальные колонки)
            analysis_df = enriched_frame.copy()
            
            # Удаляем колонки с ошибками и временными данными
            error_columns = [col for col in analysis_df.columns if col.endswith('_error')]
            analysis_df = analysis_df.drop(columns=error_columns)
            
            # Выполняем корреляционный анализ
            correlation_analysis = build_enhanced_correlation_analysis(analysis_df)
            correlation_reports = build_enhanced_correlation_reports(analysis_df)
            correlation_insights = build_correlation_insights(analysis_df)
            
            logger.info(f"Корреляционный анализ завершен. Найдено {len(correlation_insights)} инсайтов.")
            
        except Exception as exc:
            logger.warning(f"Ошибка при выполнении корреляционного анализа: {exc}")
            logger.warning(f"Тип ошибки: {type(exc).__name__}")
            # Продолжаем без корреляционного анализа

    return DocumentETLResult(
        documents=enriched_frame, 
        qc=qc,
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
    except OSError as exc:  # pragma: no cover - filesystem permission issues
        raise DocumentIOError(f"Failed to write outputs: {exc}") from exc

    outputs = {"documents": documents_path, "qc": qc_path}

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


__all__ = [
    "DocumentETLResult",
    "DocumentHTTPError",
    "DocumentIOError",
    "DocumentPipelineError",
    "DocumentQCError",
    "DocumentValidationError",
    "_add_document_sortorder_column",
    "_add_publication_date_column",
    "_create_api_client",
    "_determine_document_sortorder",
    "_determine_publication_date",
    "_extract_data_from_source",
    "read_document_input",
    "run_document_etl",
    "write_document_outputs",
]
