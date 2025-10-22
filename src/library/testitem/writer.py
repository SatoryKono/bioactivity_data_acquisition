"""Data writing stage for testitem ETL pipeline."""

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


class TestitemWriter:
    """Handles writing testitem data to output files."""

    def __init__(self, config: TestitemConfig):
        """Initialize writer with configuration."""
        self.config = config

    def write_testitem_outputs(
        self,
        result: Any,  # TestitemETLResult
        output_dir: Path,
        config: TestitemConfig
    ) -> dict[str, Path]:
        """Write testitem ETL outputs to files."""
        
        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)
        
        outputs = {}
        
        # Generate date tag
        date_tag = self._generate_date_tag(result.testitems)
        
        # Main data file
        data_path = output_dir / f"testitems_{date_tag}.csv"
        self._write_deterministic_csv(
            result.testitems,
            data_path,
            config
        )
        outputs["data"] = data_path
        
        # QC report
        qc_path = output_dir / f"testitems_{date_tag}_qc.csv"
        result.qc.to_csv(qc_path, index=False)
        outputs["qc"] = qc_path
        
        # Metadata
        meta_path = output_dir / f"testitems_{date_tag}_meta.yaml"
        self._write_metadata(result.meta, meta_path)
        outputs["meta"] = meta_path
        
        # Correlation reports (if available)
        if result.correlation_reports:
            corr_dir = output_dir / f"testitems_{date_tag}_correlation_report"
            corr_dir.mkdir(exist_ok=True)
            
            for report_name, report_df in result.correlation_reports.items():
                report_path = corr_dir / f"{report_name}.csv"
                report_df.to_csv(report_path, index=False)
                outputs[f"correlation_{report_name}"] = report_path
        
        return outputs

    def _write_deterministic_csv(
        self,
        df: pd.DataFrame,
        output_path: Path,
        config: TestitemConfig
    ) -> None:
        """Write DataFrame to CSV with deterministic formatting."""
        
        logger.info(f"Writing {len(df)} testitem records to {output_path}")
        
        # Sort data deterministically
        sorted_df = self._sort_data_deterministically(df, config)
        
        # Write using deterministic CSV writer
        write_deterministic_csv(
            sorted_df,
            output_path,
            logger
        )
        
        # Calculate and log checksum
        checksum = self._calculate_checksum(output_path)
        logger.info(f"File written successfully. Checksum: {checksum}")

    def _sort_data_deterministically(self, df: pd.DataFrame, config: TestitemConfig) -> pd.DataFrame:
        """Sort data deterministically according to configuration."""
        
        logger.info("Sorting data deterministically...")
        
        # Get sort configuration
        sort_by = getattr(config.determinism.sort, 'by', ['molecule_chembl_id', 'molregno', 'pref_name_key'])
        sort_ascending = getattr(config.determinism.sort, 'ascending', True)
        
        # Ensure all sort columns exist
        available_sort_columns = [col for col in sort_by if col in df.columns]
        
        if not available_sort_columns:
            logger.warning("No sort columns available, using default sorting by index")
            return df.sort_index()
        
        # Sort the data
        sorted_df = df.sort_values(
            by=available_sort_columns,
            ascending=sort_ascending,
            na_position='last'
        ).reset_index(drop=True)
        
        logger.info(f"Data sorted by columns: {available_sort_columns}")
        return sorted_df

    def _generate_date_tag(self, df: pd.DataFrame) -> str:
        """Generate date tag from data timestamps."""
        if df.empty:
            # Fallback to current date if no data available
            return pd.Timestamp.utcnow().strftime("%Y%m%d")
        
        # Try to get date from extracted_at column
        if 'extracted_at' in df.columns:
            extracted_at = df['extracted_at']
            if not extracted_at.empty:
                # Use the first non-null extracted_at timestamp
                first_timestamp = extracted_at.dropna().iloc[0] if not extracted_at.dropna().empty else pd.Timestamp.utcnow()
                
                # Convert to UTC date
                if isinstance(first_timestamp, str):
                    first_timestamp = pd.to_datetime(first_timestamp)
                
                return first_timestamp.strftime("%Y%m%d")
        
        # Fallback to current date
        return pd.Timestamp.utcnow().strftime("%Y%m%d")

    def _write_metadata(self, meta: dict[str, Any], output_path: Path) -> None:
        """Write metadata to YAML file."""
        
        logger.info(f"Writing metadata to {output_path}")
        
        with open(output_path, 'w', encoding='utf-8') as f:
            yaml.dump(meta, f, default_flow_style=False, allow_unicode=True)

    def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()


# Backward compatibility functions
def write_testitem_outputs(
    result: Any,  # TestitemETLResult
    output_dir: Path,
    config: TestitemConfig
) -> dict[str, Path]:
    """Write testitem ETL outputs to files (backward compatibility)."""
    writer = TestitemWriter(config)
    return writer.write_testitem_outputs(result, output_dir, config)


def calculate_checksum(file_path: Path) -> str:
    """Calculate SHA256 checksum of a file (backward compatibility)."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def generate_run_date(extracted_at: pd.Series) -> str:
    """Generate run date from extracted_at timestamps (backward compatibility)."""
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
    """Sort data deterministically according to configuration (backward compatibility)."""
    writer = TestitemWriter(config)
    return writer._sort_data_deterministically(df, config)
