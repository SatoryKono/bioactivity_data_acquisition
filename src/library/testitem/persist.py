"""Data persistence stage for testitem ETL pipeline."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from library.etl.load import write_deterministic_csv
from library.testitem.config import TestitemConfig

logger = logging.getLogger(__name__)


def calculate_checksum(file_path: Path) -> str:
    """Calculate SHA256 checksum of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def generate_run_date(extracted_at: pd.Series) -> str:
    """Generate run date from extracted_at timestamps."""
    if extracted_at.empty:
        # Fallback to current date if no timestamps available
        return pd.Timestamp.utcnow().strftime("%Y%m%d")
    
    # Use the first non-null extracted_at timestamp
    first_timestamp = extracted_at.dropna().iloc[0] if not extracted_at.dropna().empty else pd.Timestamp.utcnow()
    
    # Convert to UTC date
    if isinstance(first_timestamp, str):
        first_timestamp = pd.to_datetime(first_timestamp)
    
    return first_timestamp.strftime("%Y%m%d")


def sort_data_deterministically(df: pd.DataFrame, config: TestitemConfig) -> pd.DataFrame:
    """Sort data deterministically according to configuration."""
    
    logger.info("Sorting data deterministically...")
    
    # Get sort configuration
    sort_by = getattr(config.determinism.sort, 'by', ['molecule_chembl_id', 'molregno', 'pref_name_key'])
    ascending = getattr(config.determinism.sort, 'ascending', [True, True, True])
    na_position = getattr(config.determinism.sort, 'na_position', 'last')
    
    # Ensure we have the same number of sort fields and directions
    if len(ascending) != len(sort_by):
        # Extend ascending list with True for additional fields
        ascending = ascending + [True] * (len(sort_by) - len(ascending))
    
    # Filter to only existing columns
    existing_sort_fields = [field for field in sort_by if field in df.columns]
    existing_ascending = ascending[:len(existing_sort_fields)]
    
    if not existing_sort_fields:
        logger.warning("No sort fields found in data, using default sort by molecule_chembl_id")
        existing_sort_fields = ['molecule_chembl_id']
        existing_ascending = [True]
    
    # Sort the data
    sorted_df = df.sort_values(
        by=existing_sort_fields,
        ascending=existing_ascending,
        na_position=na_position
    ).reset_index(drop=True)
    
    logger.info(f"Data sorted by: {existing_sort_fields}")
    
    return sorted_df


def reorder_columns(df: pd.DataFrame, config: TestitemConfig) -> pd.DataFrame:
    """Reorder columns according to configuration."""
    
    logger.info("Reordering columns...")
    
    # Get column order from configuration
    column_order = getattr(config.determinism, 'column_order', [])
    
    if not column_order:
        logger.warning("No column order specified in configuration, keeping original order")
        return df
    
    # Filter to only existing columns
    existing_columns = [col for col in column_order if col in df.columns]
    
    # Add any remaining columns that weren't in the order
    remaining_columns = [col for col in df.columns if col not in existing_columns]
    final_column_order = existing_columns + remaining_columns
    
    reordered_df = df[final_column_order]
    
    logger.info(f"Columns reordered, {len(existing_columns)} columns in specified order, {len(remaining_columns)} additional columns")
    
    return reordered_df


def persist_csv(
    df: pd.DataFrame,
    output_path: Path,
    config: TestitemConfig
) -> None:
    """Persist DataFrame to CSV with deterministic formatting."""
    
    logger.info(f"Persisting CSV to: {output_path}")
    
    try:
        # Use the deterministic CSV writer
        write_deterministic_csv(
            df,
            output_path,
            determinism=config.determinism,
            output=config.io.output if hasattr(config, 'io') else None
        )
        
        logger.info(f"CSV persisted successfully: {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to persist CSV: {e}")
        raise TestitemIOError(f"Failed to persist CSV to {output_path}: {e}") from e


def generate_metadata(
    df: pd.DataFrame,
    config: TestitemConfig,
    run_date: str,
    file_checksums: dict[str, str]
) -> dict[str, Any]:
    """Generate metadata for the ETL run."""
    
    logger.info("Generating metadata...")
    
    # Calculate basic statistics
    total_records = len(df)
    
    # Count records by source
    source_counts = {}
    if "source_system" in df.columns:
        source_counts = df["source_system"].value_counts().to_dict()
    
    # Count records with PubChem data
    pubchem_count = 0
    if "pubchem_cid" in df.columns:
        pubchem_count = df["pubchem_cid"].notna().sum()
    
    # Count records with errors
    error_count = 0
    if "error" in df.columns:
        error_count = df["error"].notna().sum()
    
    # Get ChEMBL release
    chembl_release = "unknown"
    if "chembl_release" in df.columns:
        releases = df["chembl_release"].dropna().unique()
        if len(releases) > 0:
            chembl_release = str(releases[0])
    
    # Generate metadata
    metadata = {
        "pipeline_version": config.pipeline_version,
        "run_date": run_date,
        "chembl_release": chembl_release,
        "total_records": total_records,
        "source_counts": source_counts,
        "pubchem_enrichment": {
            "enabled": config.enable_pubchem,
            "records_with_pubchem_data": pubchem_count,
            "enrichment_rate": (pubchem_count / total_records * 100) if total_records > 0 else 0
        },
        "data_quality": {
            "records_with_errors": error_count,
            "error_rate": (error_count / total_records * 100) if total_records > 0 else 0
        },
        "extraction_parameters": {
            "allow_parent_missing": config.allow_parent_missing,
            "enable_pubchem": config.enable_pubchem,
            "batch_size": getattr(config.runtime, 'batch_size', 200),
            "retries": getattr(config.runtime, 'retries', 5),
            "timeout_sec": getattr(config.runtime, 'timeout_sec', 30)
        },
        "file_checksums": file_checksums,
        "artifacts": {
            "csv": {
                "filename": config.get_output_filename(run_date),
                "rows": total_records,
                "sha256": file_checksums.get("csv", "")
            }
        }
    }
    
    logger.info("Metadata generated successfully")
    
    return metadata


def persist_metadata(
    metadata: dict[str, Any],
    output_path: Path
) -> None:
    """Persist metadata to YAML file."""
    
    logger.info(f"Persisting metadata to: {output_path}")
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            yaml.dump(metadata, f, default_flow_style=False, allow_unicode=True)
        
        logger.info(f"Metadata persisted successfully: {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to persist metadata: {e}")
        raise TestitemIOError(f"Failed to persist metadata to {output_path}: {e}") from e


def generate_qc_artifacts(
    df: pd.DataFrame,
    output_dir: Path,
    run_date: str
) -> dict[str, Path]:
    """Generate QC artifacts."""
    
    logger.info("Generating QC artifacts...")
    
    qc_artifacts = {}
    
    try:
        # Create QC directory
        qc_dir = output_dir  
        qc_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate basic QC metrics
        qc_metrics = []
        
        # Total records
        qc_metrics.append({"metric": "total_records", "value": len(df)})
        
        # Source distribution
        if "source_system" in df.columns:
            source_dist = df["source_system"].value_counts().to_dict()
            qc_metrics.append({"metric": "source_distribution", "value": source_dist})
        
        # PubChem enrichment
        if "pubchem_cid" in df.columns:
            pubchem_count = df["pubchem_cid"].notna().sum()
            qc_metrics.append({"metric": "pubchem_enriched_records", "value": pubchem_count})
        
        # Error count
        if "error" in df.columns:
            error_count = df["error"].notna().sum()
            qc_metrics.append({"metric": "records_with_errors", "value": error_count})
        
        # Missing data analysis
        missing_data = {}
        for col in df.columns:
            missing_count = df[col].isna().sum()
            if missing_count > 0:
                missing_data[col] = {
                    "missing_count": int(missing_count),
                    "missing_percentage": float(missing_count / len(df) * 100)
                }
        
        if missing_data:
            qc_metrics.append({"metric": "missing_data_analysis", "value": missing_data})
        
        # Save QC metrics
        qc_df = pd.DataFrame(qc_metrics)
        qc_path = qc_dir / f"testitem_{run_date}_qc.csv"
        qc_df.to_csv(qc_path, index=False)
        qc_artifacts["qc_metrics"] = qc_path
        
        logger.info(f"QC artifacts generated: {len(qc_artifacts)} files")
        
    except Exception as e:
        logger.warning(f"Failed to generate QC artifacts: {e}")
    
    return qc_artifacts


def persist_testitem_data(
    df: pd.DataFrame,
    output_dir: Path,
    config: TestitemConfig
) -> dict[str, Path]:
    """Main persistence function for testitem data."""
    
    logger.info("Starting testitem data persistence...")
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate run date from extracted_at
    run_date = generate_run_date(df["extracted_at"] if "extracted_at" in df.columns else pd.Series())
    
    # Sort data deterministically
    sorted_df = sort_data_deterministically(df, config)
    
    # Reorder columns
    reordered_df = reorder_columns(sorted_df, config)
    
    # Generate output file paths
    csv_filename = config.get_output_filename(run_date)
    csv_path = output_dir / csv_filename
    meta_filename = config.get_meta_filename(run_date)
    meta_path = output_dir / meta_filename
    
    # Persist CSV
    persist_csv(reordered_df, csv_path, config)
    
    # Calculate file checksums
    file_checksums = {
        "csv": calculate_checksum(csv_path)
    }
    
    # Generate and persist metadata
    metadata = generate_metadata(reordered_df, config, run_date, file_checksums)
    persist_metadata(metadata, meta_path)
    
    # Generate QC artifacts
    qc_artifacts = generate_qc_artifacts(reordered_df, output_dir, run_date)
    
    # Prepare result paths
    result_paths = {
        "csv": csv_path,
        "meta": meta_path
    }
    result_paths.update(qc_artifacts)
    
    logger.info("Testitem data persistence completed")
    
    return result_paths


class TestitemIOError(Exception):
    """Raised when testitem I/O operations fail."""
    pass
