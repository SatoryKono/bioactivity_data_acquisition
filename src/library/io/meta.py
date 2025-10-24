"""Metadata handling for ETL pipelines."""

from __future__ import annotations

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
    
    def to_dict(self, config: APIClientConfig | None = None) -> dict[str, Any]:
        """Convert metadata to dictionary format for YAML serialization.
        
        Args:
            config: API client configuration for ChEMBL
            
        Returns:
            Dictionary with all metadata fields
        """
        chembl_status = self.get_chembl_status(config)
        
        meta = {
            "dataset": self.dataset,
            "run_id": self._run_id,
            "generated_at": self._generated_at,
            "chembl_release": chembl_status.get("chembl_release"),
        }
        
        # Add additional metadata if available
        if chembl_status.get("status"):
            meta["chembl_status"] = chembl_status["status"]
        
        if chembl_status.get("timestamp"):
            meta["chembl_status_timestamp"] = chembl_status["timestamp"]
        
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
