"""Output writer for testitem ETL artefacts."""

from __future__ import annotations

from pathlib import Path

from library.common.pipeline_base import ETLResult
from library.common.writer_base import TestitemETLWriter

from .config import TestitemConfig


def write_testitem_outputs(
    result: ETLResult,
    output_dir: Path,
    date_tag: str,
    config: TestitemConfig
) -> dict[str, Path]:
    """Write testitem ETL outputs to files with standardized naming.
    
    Standardized artifact names:
    - testitems_<date_tag>.csv              # Main data
    - testitems_<date_tag>.meta.yaml        # Metadata
    - testitems_<date_tag>_qc_summary.csv   # QC summary
    - testitems_<date_tag>_qc_detailed.csv  # QC detailed (optional)
    - testitems_<date_tag>_rejected.csv     # Rejected records (optional)
    - testitems_<date_tag>_correlation.csv  # Correlation analysis (optional)
    """
    
    # Use the new unified ETL writer
    writer = TestitemETLWriter(config, "testitems")
    return writer.write_outputs(result, output_dir, date_tag)


__all__ = [
    "write_testitem_outputs",
]
