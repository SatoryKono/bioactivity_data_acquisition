"""Simplified document ETL pipeline orchestration."""

from __future__ import annotations

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
from library.tools.citation_formatter import add_citation_column
from library.tools.journal_normalizer import normalize_journal_columns
from library.etl.enhanced_correlation import (
    build_enhanced_correlation_analysis,
    build_enhanced_correlation_reports,
    build_correlation_insights
)


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


def _create_api_client(source: str, config: DocumentConfig) -> Any:
    """Create an API client for the specified source."""
    from library.config import RetrySettings
    
    # ChEMBL API is slower, so increase timeout
    timeout = config.http.global_.timeout_sec
    if source == "chembl":
        timeout = max(timeout, 60.0)  # At least 60 seconds for ChEMBL
    
    # Create base API client config
    api_config = APIClientConfig(
        name=source,
        base_url=_get_base_url(source),
        headers=_get_headers(source),
        timeout=timeout,
        retries=RetrySettings(
            total=config.http.global_.retries.total,
            backoff_multiplier=2.0
        ),
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
        "chembl_title": None, "chembl_doi": None, "chembl_pubmed_id": None,
        "chembl_journal": None, "chembl_year": None, "chembl_volume": None, 
        "chembl_issue": None,
        # Crossref columns  
        "crossref_title": None, "crossref_doc_type": None, "crossref_subject": None, 
        "crossref_error": None,
        # OpenAlex columns
        "openalex_doi_key": None, "openalex_title": None, "openalex_doc_type": None,
        "openalex_type_crossref": None, "openalex_publication_year": None, 
        "openalex_error": None,
        # PubMed columns
        "pubmed_pmid": None, "pubmed_doi": None, "pubmed_article_title": None, 
        "pubmed_abstract": None, "pubmed_journal_title": None, "pubmed_volume": None, 
        "pubmed_issue": None, "pubmed_start_page": None, "pubmed_end_page": None, 
        "pubmed_publication_type": None, "pubmed_mesh_descriptors": None, 
        "pubmed_mesh_qualifiers": None, "pubmed_chemical_list": None,
        "pubmed_year_completed": None, "pubmed_month_completed": None, 
        "pubmed_day_completed": None, "pubmed_year_revised": None, 
        "pubmed_month_revised": None, "pubmed_day_revised": None,
        "pubmed_issn": None, "pubmed_error": None,
        # Semantic Scholar columns
        "semantic_scholar_pmid": None, "semantic_scholar_doi": None, 
        "semantic_scholar_semantic_scholar_id": None,
        "semantic_scholar_publication_types": None, "semantic_scholar_venue": None,
        "semantic_scholar_external_ids": None, "semantic_scholar_error": None,
        # Common columns
        "doc_type": None, "doi_key": None
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
                if pd.notna(row.get("document_chembl_id")):
                    data = client.fetch_by_doc_id(str(row["document_chembl_id"]))
                    # Remove source from data to avoid overwriting
                    data.pop("source", None)
                    # Only update non-None values to preserve existing data
                    for key, value in data.items():
                        if value is not None:
                            row_data[key] = value
                    
            elif source == "crossref":
                if pd.notna(row.get("doi")):
                    data = client.fetch_by_doi(str(row["doi"]))
                    data.pop("source", None)
                    for key, value in data.items():
                        if value is not None:
                            row_data[key] = value
                elif pd.notna(row.get("pubmed_id")):
                    data = client.fetch_by_pmid(str(row["pubmed_id"]))
                    data.pop("source", None)
                    for key, value in data.items():
                        if value is not None:
                            row_data[key] = value
                    
            elif source == "openalex":
                if pd.notna(row.get("doi")):
                    data = client.fetch_by_doi(str(row["doi"]))
                    data.pop("source", None)
                    for key, value in data.items():
                        if value is not None:
                            row_data[key] = value
                elif pd.notna(row.get("pubmed_id")):
                    data = client.fetch_by_pmid(str(row["pubmed_id"]))
                    data.pop("source", None)
                    for key, value in data.items():
                        if value is not None:
                            row_data[key] = value
                    
            elif source == "pubmed":
                if pd.notna(row.get("pubmed_id")):
                    data = client.fetch_by_pmid(str(row["pubmed_id"]))
                    data.pop("source", None)
                    for key, value in data.items():
                        if value is not None:
                            row_data[key] = value
                    
            elif source == "semantic_scholar":
                if pd.notna(row.get("pubmed_id")):
                    data = client.fetch_by_pmid(str(row["pubmed_id"]))
                    data.pop("source", None)
                    for key, value in data.items():
                        if value is not None:
                            row_data[key] = value
            
            enriched_data.append(row_data)
                    
        except Exception as exc:
            # Log error but continue processing other records
            doc_id = row.get('document_chembl_id', 'unknown')
            print(f"Error extracting data from {source} for row {doc_id}: {exc}")
            print(f"Error type: {type(exc).__name__}")
            
            # Ensure error row also has all columns
            error_row = row.to_dict()
            error_row.update(default_columns)
            
            # Set error flag for this source with more detailed error info
            error_msg = f"{type(exc).__name__}: {str(exc)}"
            
            # Специальная обработка для ошибок rate limiting
            if "429" in str(exc) or "Rate limited" in str(exc):
                error_msg = f"Rate limited by API: {str(exc)}"
                print(f"Rate limiting detected for {source}, continuing with next record...")
            
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
    normalised = normalised.sort_values("document_chembl_id").reset_index(drop=True)
    return normalised


def _initialize_all_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Initialize all possible output columns with default values."""
    
    # Define all possible columns that should exist in the output
    all_columns = {
        # Original ChEMBL fields
        "document_chembl_id", "title", "doi", "pubmed_id", "doc_type", "journal", "year",
        # Legacy ChEMBL fields
        "abstract", "authors", "classification", "document_contains_external_links",
        "first_page", "is_experimental_doc", "issue", "last_page", "month", "volume",
        # Enriched fields from external sources
        # source column removed - not needed in final output
        # ChEMBL-specific fields
        "chembl_title", "chembl_doi", "chembl_pubmed_id", "chembl_journal", 
        "chembl_year", "chembl_volume", "chembl_issue",
        # Crossref-specific fields
        "crossref_title", "crossref_doc_type", "crossref_subject", "crossref_error",
        # OpenAlex-specific fields
        "openalex_doi_key", "openalex_title", "openalex_doc_type", 
        "openalex_type_crossref", "openalex_publication_year", "openalex_error",
        # PubMed-specific fields
        "pubmed_pmid", "pubmed_doi", "pubmed_article_title", "pubmed_abstract",
        "pubmed_journal_title", "pubmed_volume", "pubmed_issue", "pubmed_start_page", 
        "pubmed_end_page", "pubmed_publication_type", "pubmed_mesh_descriptors", 
        "pubmed_mesh_qualifiers", "pubmed_chemical_list", "pubmed_year_completed",
        "pubmed_month_completed", "pubmed_day_completed", "pubmed_year_revised",
        "pubmed_month_revised", "pubmed_day_revised", "pubmed_issn", "pubmed_error",
        # Semantic Scholar-specific fields
        "semantic_scholar_pmid", "semantic_scholar_doi", "semantic_scholar_semantic_scholar_id",
        "semantic_scholar_publication_types", "semantic_scholar_venue", 
        "semantic_scholar_external_ids", "semantic_scholar_error",
        # Common fields
        "doi_key",
        # Citation field
        "citation"
    }
    
    # Add missing columns with default values
    for column in all_columns:
        if column not in frame.columns:
            frame[column] = None
    
    # Ensure doc_type has a default value for all rows
    if "doc_type" in frame.columns:
        frame["doc_type"] = frame["doc_type"].fillna("PUBLICATION")
    
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
            print(f"Extracting data from {source}...")
            client = _create_api_client(source, config)
            
            # Для источников с высоким rate limiting добавляем дополнительную задержку
            if source in ["semantic_scholar", "openalex"]:
                print(f"Using conservative rate limiting for {source} (no API key)")
                import time
                if source == "semantic_scholar":
                    time.sleep(10)  # Дополнительная задержка для Semantic Scholar
                else:
                    time.sleep(2)  # Дополнительная задержка для OpenAlex
            
            enriched_frame = _extract_data_from_source(source, client, enriched_frame, config)
            
            # Log success statistics
            if source == "chembl":
                success_count = enriched_frame["chembl_title"].notna().sum()
            elif source == "crossref":
                success_count = enriched_frame["crossref_title"].notna().sum()
            elif source == "openalex":
                success_count = enriched_frame["openalex_title"].notna().sum()
            elif source == "pubmed":
                success_count = enriched_frame["pubmed_pmid"].notna().sum()
            elif source == "semantic_scholar":
                success_count = enriched_frame["semantic_scholar_pmid"].notna().sum()
            else:
                success_count = 0
                
            print(f"Successfully extracted data from {source}: "
                  f"{success_count}/{len(enriched_frame)} records")
                  
            # Для источников с высоким rate limiting добавляем задержку после завершения
            if source in ["semantic_scholar", "openalex"]:
                print(f"Waiting before next source to respect rate limits...")
                import time
                if source == "semantic_scholar":
                    time.sleep(15)  # Дополнительная задержка для Semantic Scholar
                else:
                    time.sleep(5)  # Задержка для OpenAlex
                
        except Exception as exc:
            print(f"Warning: Failed to extract data from {source}: {exc}")
            print(f"Error type: {type(exc).__name__}")
            # Continue with other sources even if one fails

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
            source_data_count = len(enriched_frame[enriched_frame["crossref_title"].notna()])
        elif source == "pubmed":
            if "pubmed_pmid" in enriched_frame.columns:
                source_data_count = len(enriched_frame[enriched_frame["pubmed_pmid"].notna()])
            else:
                source_data_count = 0
        elif source == "semantic_scholar":
            if "scholar_pmid" in enriched_frame.columns:
                source_data_count = len(enriched_frame[enriched_frame["scholar_pmid"].notna()])
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
    if hasattr(config, 'postprocess') and hasattr(config.postprocess, 'correlation') and config.postprocess.correlation.enabled:
        try:
            print("Выполняем корреляционный анализ документов...")
            
            # Создаем временный DataFrame для анализа (только числовые и категориальные колонки)
            analysis_df = enriched_frame.copy()
            
            # Удаляем колонки с ошибками и временными данными
            error_columns = [col for col in analysis_df.columns if col.endswith('_error')]
            analysis_df = analysis_df.drop(columns=error_columns)
            
            # Выполняем корреляционный анализ
            correlation_analysis = build_enhanced_correlation_analysis(analysis_df)
            correlation_reports = build_enhanced_correlation_reports(analysis_df)
            correlation_insights = build_correlation_insights(analysis_df)
            
            print(f"Корреляционный анализ завершен. Найдено {len(correlation_insights)} инсайтов.")
            
        except Exception as exc:
            print(f"Предупреждение: Ошибка при выполнении корреляционного анализа: {exc}")
            print(f"Тип ошибки: {type(exc).__name__}")
            # Продолжаем без корреляционного анализа

    return DocumentETLResult(
        documents=enriched_frame, 
        qc=qc,
        correlation_analysis=correlation_analysis,
        correlation_reports=correlation_reports,
        correlation_insights=correlation_insights
    )


def write_document_outputs(
    result: DocumentETLResult, output_dir: Path, date_tag: str
) -> dict[str, Path]:
    """Persist ETL artefacts to disk and return the generated paths."""

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:  # pragma: no cover - filesystem permission issues
        raise DocumentIOError(f"Failed to create output directory: {exc}") from exc

    documents_path = output_dir / f"documents_{date_tag}.csv"
    qc_path = output_dir / f"documents_{date_tag}_qc.csv"

    try:
        # Добавляем колонку с литературными ссылками перед сохранением
        documents_with_citations = add_citation_column(result.documents)
        
        # Нормализуем колонки с названиями журналов
        documents_normalized = normalize_journal_columns(documents_with_citations)
        
        documents_normalized.to_csv(documents_path, index=False)
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
            
            print(f"Корреляционные отчеты сохранены в: {correlation_dir}")
            
        except Exception as exc:
            print(f"Предупреждение: Ошибка при сохранении корреляционных отчетов: {exc}")
            print(f"Тип ошибки: {type(exc).__name__}")

    return outputs


__all__ = [
    "DocumentETLResult",
    "DocumentHTTPError",
    "DocumentIOError",
    "DocumentPipelineError",
    "DocumentQCError",
    "DocumentValidationError",
    "_create_api_client",
    "_extract_data_from_source",
    "read_document_input",
    "run_document_etl",
    "write_document_outputs",
]
