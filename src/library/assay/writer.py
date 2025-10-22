"""Output writer for assay ETL artefacts."""

from __future__ import annotations

import hashlib
import logging
import yaml
from pathlib import Path
from typing import Any

import pandas as pd

from library.common.pipeline_base import ETLResult
from library.etl.load import write_deterministic_csv

from .config import AssayConfig

logger = logging.getLogger(__name__)


def _calculate_checksum(file_path: Path) -> str:
    """Calculate SHA256 checksum of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def write_assay_outputs(
    result: ETLResult,
    output_dir: Path,
    date_tag: str,
    config: AssayConfig,
) -> dict[str, Path]:
    """Persist ETL artefacts to disk and return the generated paths."""

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:  # pragma: no cover - filesystem permission issues
        raise OSError(f"Failed to create output directory: {exc}") from exc

    # File paths
    csv_path = output_dir / f"assays_{date_tag}.csv"
    qc_path = output_dir / f"assays_{date_tag}_qc_summary.csv"
    meta_path = output_dir / f"assays_{date_tag}.meta.yaml"

    try:
        # S06: Persist data with deterministic serialization
        logger.info("S06: Persisting data...")

        # Save CSV with deterministic order
        write_deterministic_csv(result.data, csv_path, determinism=config.determinism, output=config.io.output)

        # Save QC data (всегда создаём файл)
        if isinstance(result.qc_summary, pd.DataFrame) and not result.qc_summary.empty:
            result.qc_summary.to_csv(qc_path, index=False)
        else:
            pd.DataFrame([{"metric": "row_count", "value": int(len(result.data))}]).to_csv(qc_path, index=False)

        # Save metadata
        with open(meta_path, "w", encoding="utf-8") as f:
            yaml.dump(result.meta, f, default_flow_style=False, allow_unicode=True)

        # Save correlation reports if available - consolidated into single file
        correlation_paths = {}
        if result.correlation_reports:
            try:
                corr_path = output_dir / f"assays_{date_tag}_correlation.csv"
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
                    correlation_paths["correlation"] = corr_path

                logger.info(f"Saved correlation analysis to {corr_path}")

            except Exception as exc:
                logger.warning(f"Failed to save correlation reports: {exc}")

        # Save correlation insights if available
        try:
            if result.correlation_insights:
                insights_path = output_dir / f"assays_{date_tag}_correlation_insights.json"
                import json as _json

                with insights_path.open("w", encoding="utf-8") as f:
                    _json.dump(result.correlation_insights, f, ensure_ascii=False, indent=2)
                correlation_paths["correlation_insights"] = insights_path
        except Exception:
            logger.warning("Failed to save correlation insights")

        # Add file checksums to metadata
        if result.meta is not None:
            result.meta["file_checksums"] = {
                "csv": _calculate_checksum(csv_path),
                "qc": _calculate_checksum(qc_path),
            }

        # Update metadata file with checksums
        with open(meta_path, "w", encoding="utf-8") as f:
            yaml.dump(result.meta, f, default_flow_style=False, allow_unicode=True)

    except OSError as exc:  # pragma: no cover - filesystem permission issues
        raise OSError(f"Failed to write outputs: {exc}") from exc

    result_paths: dict[str, Any] = {"csv": csv_path, "qc": qc_path, "meta": meta_path}

    # Add correlation report paths if available
    if correlation_paths:
        result_paths["correlation_reports"] = correlation_paths

    return result_paths


__all__ = [
    "write_assay_outputs",
]
