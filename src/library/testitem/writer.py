"""Output writer for testitem ETL artefacts."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd
import yaml

from library.common.pipeline_base import ETLResult
from library.etl.load import write_deterministic_csv

from .config import TestitemConfig


def _calculate_checksum(file_path: Path) -> str:
    """Calculate SHA256 checksum of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


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
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    outputs = {}
    
    # Main data file
    data_path = output_dir / f"testitems_{date_tag}.csv"
    write_deterministic_csv(
        result.data,
        data_path,
        determinism=config.determinism
    )
    outputs["main"] = data_path
    
    # QC summary report
    qc_path = output_dir / f"testitems_{date_tag}_qc_summary.csv"
    if isinstance(result.qc_summary, pd.DataFrame) and not result.qc_summary.empty:
        result.qc_summary.to_csv(qc_path, index=False)
    else:
        pd.DataFrame([{"metric": "row_count", "value": int(len(result.data))}]).to_csv(qc_path, index=False)
    outputs["qc_summary"] = qc_path
    
    # Metadata
    meta_path = output_dir / f"testitems_{date_tag}.meta.yaml"
    meta_data = result.meta if result.meta is not None else {}
    with open(meta_path, 'w', encoding='utf-8') as f:
        yaml.dump(meta_data, f, default_flow_style=False, allow_unicode=True)
    outputs["metadata"] = meta_path
    
    # Correlation reports (if available) - consolidated into single file
    if result.correlation_reports:
        corr_path = output_dir / f"testitems_{date_tag}_correlation.csv"
        # Combine all correlation reports into single DataFrame
        correlation_data = []
        for report_name, report_df in result.correlation_reports.items():
            if not report_df.empty:
                report_df_copy = report_df.copy()
                report_df_copy['report_type'] = report_name
                correlation_data.append(report_df_copy)
        
        if correlation_data:
            combined_corr = pd.concat(correlation_data, ignore_index=True)
            combined_corr.to_csv(corr_path, index=False)
            outputs["correlation"] = corr_path
    
    # Add file checksums to metadata
    if result.meta is not None:
        result.meta["file_checksums"] = {
            "csv": _calculate_checksum(data_path),
            "qc_summary": _calculate_checksum(qc_path),
        }
    
    # Update metadata file with checksums
    with open(meta_path, 'w', encoding='utf-8') as f:
        yaml.dump(meta_data, f, default_flow_style=False, allow_unicode=True)
    
    return outputs


__all__ = [
    "write_testitem_outputs",
]
