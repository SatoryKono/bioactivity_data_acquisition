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
    
    for _, row in frame.iterrows():
        try:
            # Start with the original row data
            row_data = row.to_dict()
            
            if source == "chembl":
                if pd.notna(row.get("document_chembl_id")):
                    data = client.fetch_by_doc_id(str(row["document_chembl_id"]))
                    # Remove source from data to avoid overwriting
                    data.pop("source", None)
                    row_data.update(data)
                    
            elif source == "crossref":
                if pd.notna(row.get("doi")):
                    data = client.fetch_by_doi(str(row["doi"]))
                    data.pop("source", None)
                    row_data.update(data)
                elif pd.notna(row.get("pubmed_id")):
                    data = client.fetch_by_pmid(str(row["pubmed_id"]))
                    data.pop("source", None)
                    row_data.update(data)
                    
            elif source == "openalex":
                if pd.notna(row.get("doi")):
                    data = client.fetch_by_doi(str(row["doi"]))
                    data.pop("source", None)
                    row_data.update(data)
                elif pd.notna(row.get("pubmed_id")):
                    data = client.fetch_by_pmid(str(row["pubmed_id"]))
                    data.pop("source", None)
                    row_data.update(data)
                    
            elif source == "pubmed":
                if pd.notna(row.get("pubmed_id")):
                    data = client.fetch_by_pmid(str(row["pubmed_id"]))
                    data.pop("source", None)
                    row_data.update(data)
                    
            elif source == "semantic_scholar":
                if pd.notna(row.get("pubmed_id")):
                    data = client.fetch_by_pmid(str(row["pubmed_id"]))
                    data.pop("source", None)
                    row_data.update(data)
            
            enriched_data.append(row_data)
                    
        except Exception as exc:
            # Log error but continue processing other records
            doc_id = row.get('document_chembl_id', 'unknown')
            print(f"Error extracting data from {source} for row {doc_id}: {exc}")
            enriched_data.append(row.to_dict())
    
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


def run_document_etl(config: DocumentConfig, frame: pd.DataFrame) -> DocumentETLResult:
    """Execute the document ETL pipeline returning enriched artefacts."""

    normalised = _normalise_columns(frame)

    if config.runtime.limit is not None:
        normalised = normalised.head(config.runtime.limit)

    duplicates = normalised["document_chembl_id"].duplicated()
    if bool(duplicates.any()):
        raise DocumentQCError("Duplicate document_chembl_id values detected")

    # Extract data from enabled sources
    enriched_frame = normalised.copy()
    enabled_sources = config.enabled_sources()
    
    for source in enabled_sources:
        try:
            print(f"Extracting data from {source}...")
            client = _create_api_client(source, config)
            enriched_frame = _extract_data_from_source(source, client, enriched_frame, config)
            print(f"Successfully extracted data from {source}")
        except Exception as exc:
            print(f"Warning: Failed to extract data from {source}: {exc}")
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

    return DocumentETLResult(documents=enriched_frame, qc=qc)


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
        result.documents.to_csv(documents_path, index=False)
        result.qc.to_csv(qc_path, index=False)
    except OSError as exc:  # pragma: no cover - filesystem permission issues
        raise DocumentIOError(f"Failed to write outputs: {exc}") from exc

    return {"documents": documents_path, "qc": qc_path}


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
