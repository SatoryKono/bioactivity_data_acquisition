"""Output writer for document ETL artefacts."""

from __future__ import annotations

from pathlib import Path

from library.common.writer_base import DocumentETLWriter, ETLResult

from .config import DocumentConfig


def write_document_outputs(
    result: ETLResult,
    output_dir: Path,
    date_tag: str,
    config: DocumentConfig | dict
) -> dict[str, Path]:
    """Write document ETL outputs to files with standardized naming.
    
    Standardized artifact names:
    - documents_<date_tag>.csv              # Main data
    - documents_<date_tag>.meta.yaml        # Metadata
    - documents_<date_tag>_qc_summary.csv   # QC summary
    - documents_<date_tag>_qc_detailed.csv  # QC detailed (optional)
    - documents_<date_tag>_rejected.csv     # Rejected records (optional)
    - documents_<date_tag>_correlation.csv  # Correlation analysis (optional)
    """
    
    # Convert dict config to DocumentConfig if needed
    if isinstance(config, dict):
        from .config import DocumentConfig
        config = DocumentConfig(**config)
    
    # Use the new unified ETL writer
    writer = DocumentETLWriter(config, "documents")
    return writer.write_outputs(result, output_dir, date_tag)


__all__ = [
    "write_document_outputs",
]
