"""Simplified document ETL pipeline orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

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

    qc = pd.DataFrame(
        [
            {"metric": "row_count", "value": int(len(normalised))},
            {"metric": "enabled_sources", "value": len(config.enabled_sources())},
        ]
    )

    return DocumentETLResult(documents=normalised, qc=qc)


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
    "read_document_input",
    "run_document_etl",
    "write_document_outputs",
]
