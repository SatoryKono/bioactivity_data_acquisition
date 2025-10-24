"""Metadata handling for ETL pipelines."""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from structlog.stdlib import BoundLogger

from library.clients.chembl import ChEMBLClient
from library.config import APIClientConfig


class DatasetMetadata:
    """Handles dataset metadata including ChEMBL version information."""
    
    def __init__(self, dataset_name: str = "chembl", logger: BoundLogger | None = None):
        self.dataset = dataset_name
        self.logger = logger
        self._chembl_status: dict[str, Any] | None = None
        self._run_id = str(uuid.uuid4())
        self._generated_at = datetime.utcnow().isoformat() + "Z"
        self._started_at: str | None = None
        self._completed_at: str | None = None
        self._duration_sec: float | None = None
        self._row_count: int | None = None
        self._row_count_accepted: int | None = None
        self._row_count_rejected: int | None = None
        self._columns_count: int | None = None
        self._schema_passed: bool | None = None
        self._qc_passed: bool | None = None
        self._warnings: int | None = None
        self._errors: int | None = None
        self._files: dict[str, str] = {}
        self._checksums: dict[str, str] = {}
    
    def get_chembl_status(self, config: APIClientConfig | None = None) -> dict[str, Any]:
        """Get ChEMBL status information, caching the result for the run.
        
        Args:
            config: API client configuration for ChEMBL
            
        Returns:
            Dictionary with ChEMBL version and release information
        """
        if self._chembl_status is None:
            if config is None:
                # Create default config if none provided
                from library.config import RetrySettings
                config = APIClientConfig(
                    name="chembl",
                    base_url="https://www.ebi.ac.uk/chembl/api/data",
                    timeout=30,
                    retries=RetrySettings(total=3)
                )
            
            try:
                client = ChEMBLClient(config)
                self._chembl_status = client.get_chembl_status()
                
                if self.logger:
                    self.logger.info(
                        f"Retrieved ChEMBL status: release={self._chembl_status.get('chembl_release')}, status={self._chembl_status.get('status')}"
                    )
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Failed to get ChEMBL status: {e}")
                self._chembl_status = {
                    "chembl_release": None,
                    "status": "error",
                    "error": str(e)
                }
        
        return self._chembl_status
    
    def set_execution_info(self, started_at: str, completed_at: str, duration_sec: float) -> None:
        """Set execution timing information."""
        self._started_at = started_at
        self._completed_at = completed_at
        self._duration_sec = duration_sec
    
    def set_data_info(self, row_count: int, row_count_accepted: int, row_count_rejected: int, columns_count: int) -> None:
        """Set data statistics."""
        self._row_count = row_count
        self._row_count_accepted = row_count_accepted
        self._row_count_rejected = row_count_rejected
        self._columns_count = columns_count
    
    def set_validation_info(self, schema_passed: bool, qc_passed: bool, warnings: int, errors: int) -> None:
        """Set validation results."""
        self._schema_passed = schema_passed
        self._qc_passed = qc_passed
        self._warnings = warnings
        self._errors = errors
    
    def add_file(self, file_type: str, file_path: str) -> None:
        """Add a file to the metadata."""
        self._files[file_type] = file_path
    
    def add_checksum(self, file_path: str) -> None:
        """Calculate and add checksums for a file."""
        if not Path(file_path).exists():
            return
        
        with open(file_path, 'rb') as f:
            content = f.read()
            md5_hash = hashlib.md5(content).hexdigest()
            sha256_hash = hashlib.sha256(content).hexdigest()
            
            filename = Path(file_path).name
            self._checksums[f"{filename}_md5"] = md5_hash
            self._checksums[f"{filename}_sha256"] = sha256_hash
    
    def to_dict(self, config: APIClientConfig | None = None) -> dict[str, Any]:
        """Convert metadata to dictionary format for YAML serialization.
        
        Args:
            config: API client configuration for ChEMBL
            
        Returns:
            Dictionary with all metadata fields
        """
        chembl_status = self.get_chembl_status(config)
        
        # Build simplified metadata structure
        meta = {
            "pipeline": {
                "name": self.dataset,
                "version": "2.0.0",
                "entity_type": self.dataset,
                "source_system": "chembl"
            },
            "execution": {
                "run_id": self._run_id,
                "started_at": self._started_at,
                "completed_at": self._completed_at,
                "duration_sec": self._duration_sec
            },
            "data": {
                "row_count": self._row_count,
                "row_count_accepted": self._row_count_accepted,
                "row_count_rejected": self._row_count_rejected,
                "columns_count": self._columns_count
            },
            "sources": [
                {
                    "name": "chembl",
                    "version": chembl_status.get("chembl_release"),
                    "records": self._row_count
                }
            ] if chembl_status.get("chembl_release") else [],
            "validation": {
                "schema_passed": self._schema_passed,
                "qc_passed": self._qc_passed,
                "warnings": self._warnings,
                "errors": self._errors
            },
            "files": self._files,
            "checksums": self._checksums
        }
        
        return meta
    
    def save_to_file(self, file_path: Path, config: APIClientConfig | None = None) -> None:
        """Save metadata to YAML file.
        
        Args:
            file_path: Path to save the metadata file
            config: API client configuration for ChEMBL
        """
        meta_dict = self.to_dict(config)
        
        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write YAML file
        with file_path.open("w", encoding="utf-8") as f:
            yaml.dump(meta_dict, f, default_flow_style=False, allow_unicode=True)
        
        if self.logger:
            self.logger.info(f"Saved metadata to {file_path}")


def create_dataset_metadata(
    dataset_name: str = "chembl",
    config: APIClientConfig | None = None,
    logger: BoundLogger | None = None
) -> DatasetMetadata:
    """Create a new dataset metadata instance.
    
    Args:
        dataset_name: Name of the dataset
        config: API client configuration for ChEMBL
        logger: Logger instance
        
    Returns:
        DatasetMetadata instance
    """
    return DatasetMetadata(dataset_name, logger)


__all__ = ["DatasetMetadata", "create_dataset_metadata"]
