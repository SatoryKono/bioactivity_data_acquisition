"""Refactored assay ETL pipeline using PipelineBase."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from library.assay.config import AssayConfig
from library.assay.client import AssayChEMBLClient
from library.common.pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class AssayPipeline(PipelineBase[AssayConfig]):
    """Assay ETL pipeline using unified PipelineBase."""
    
    def __init__(self, config: AssayConfig) -> None:
        """Initialize assay pipeline with configuration."""
        super().__init__(config)
    
    def _setup_clients(self) -> None:
        """Initialize HTTP clients for assay sources."""
        self.clients = {}
        
        # ChEMBL client
        if "chembl" in self.config.sources and self.config.sources["chembl"].enabled:
            self.clients["chembl"] = self._create_chembl_client()
    
    def _create_chembl_client(self) -> AssayChEMBLClient:
        """Create ChEMBL client."""
        from library.config import APIClientConfig, RateLimitSettings, RetrySettings
        
        source_config = self.config.sources["chembl"]
        timeout = source_config.http.timeout_sec or self.config.http.global_.timeout_sec
        timeout = max(timeout, 60.0)  # At least 60 seconds for ChEMBL
        
        headers = self._get_headers("chembl")
        headers.update(self.config.http.global_.headers)
        headers.update(source_config.http.headers)
        
        processed_headers = self._process_headers(headers)
        
        client_config = APIClientConfig(
            name="chembl",
            base_url=source_config.http.base_url or "https://www.ebi.ac.uk/chembl/api/data",
            timeout_sec=timeout,
            retries=RetrySettings(
                total=self.config.http.global_.retries.total,
                backoff_multiplier=self.config.http.global_.retries.backoff_multiplier,
                backoff_max=60.0,  # Default backoff max
            ),
            rate_limit=RateLimitSettings(
                max_calls=self.config.http.global_.rate_limit.get("max_calls", 10),
                period=self.config.http.global_.rate_limit.get("period", 1.0),
            ),
            headers=processed_headers,
            verify_ssl=True,
            follow_redirects=True,
        )
        
        return AssayChEMBLClient(client_config)
    
    def _get_headers(self, source: str) -> dict[str, str]:
        """Get default headers for a source."""
        return {
            "Accept": "application/json",
            "User-Agent": "bioactivity-data-acquisition/0.1.0",
        }
    
    def _process_headers(self, headers: dict[str, str]) -> dict[str, str]:
        """Process headers with secret placeholders."""
        import os
        processed = {}
        for key, value in headers.items():
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                env_var = value[2:-1]
                processed[key] = os.getenv(env_var, value)
            else:
                processed[key] = value
        return processed
    
    def extract(self, input_data: pd.DataFrame) -> pd.DataFrame:
        """Extract assay data from ChEMBL."""
        logger.info(f"Extracting assay data for {len(input_data)} assays")
        
        # Validate input data (placeholder)
        validated_data = input_data
        
        # Apply limit if specified
        if self.config.runtime.limit is not None:
            validated_data = validated_data.head(self.config.runtime.limit)
        
        # Check for duplicates
        duplicates = validated_data["assay_chembl_id"].duplicated()
        if duplicates.any():
            raise ValueError("Duplicate assay_chembl_id values detected")
        
        # Extract data from ChEMBL
        if "chembl" in self.clients:
            try:
                logger.info("Extracting data from ChEMBL")
                chembl_data = self._extract_from_chembl(validated_data)
                extracted_data = self._merge_chembl_data(validated_data, chembl_data)
            except Exception as e:
                logger.error(f"Failed to extract from ChEMBL: {e}")
                if not self.config.runtime.allow_incomplete_sources:
                    raise
        else:
            extracted_data = validated_data
        
        logger.info(f"Extracted data for {len(extracted_data)} assays")
        return extracted_data
    
    def _extract_from_chembl(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract data from ChEMBL."""
        logger.info(f"Extracting assay data from ChEMBL for {len(data)} assays")
        
        extracted_records = []
        chembl_client = self.clients["chembl"]
        
        for _, row in data.iterrows():
            assay_id = row["assay_chembl_id"]
            try:
                assay_data = chembl_client.fetch_by_assay_id(assay_id)
                if assay_data:
                    extracted_records.append(assay_data)
            except Exception as e:
                logger.warning(f"Failed to fetch assay {assay_id}: {e}")
                continue
        
        if extracted_records:
            extracted_df = pd.DataFrame(extracted_records)
            logger.info(f"Successfully extracted {len(extracted_df)} assay records from ChEMBL")
            return extracted_df
        else:
            logger.warning("No assay data extracted from ChEMBL")
            return pd.DataFrame()
    
    def _merge_chembl_data(self, base_data: pd.DataFrame, chembl_data: pd.DataFrame) -> pd.DataFrame:
        """Merge ChEMBL data into base data."""
        if chembl_data.empty:
            logger.warning("No ChEMBL data to merge")
            return base_data
        
        # Merge on assay_chembl_id
        merged_data = base_data.merge(
            chembl_data, 
            on="assay_chembl_id", 
            how="left",
            suffixes=("", "_chembl")
        )
        
        logger.info(f"Merged ChEMBL data: {len(merged_data)} records")
        return merged_data
    
    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Normalize assay data."""
        logger.info("Normalizing assay data")
        
        # Apply assay normalization (placeholder)
        normalized_data = raw_data
        
        logger.info(f"Normalized {len(normalized_data)} assays")
        return normalized_data
    
    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate assay data."""
        logger.info("Validating assay data")
        
        # Validate normalized data (placeholder)
        validated_data = data
        
        logger.info(f"Validated {len(validated_data)} assays")
        return validated_data
    
    def filter_quality(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Filter assays by quality."""
        logger.info("Filtering assays by quality")
        
        # Apply quality filters (placeholder - accept all data)
        accepted_data = data
        rejected_data = pd.DataFrame()
        
        logger.info(f"Quality filtering: {len(accepted_data)} accepted, {len(rejected_data)} rejected")
        return accepted_data, rejected_data
    
    def _build_metadata(self, data: pd.DataFrame) -> dict[str, Any]:
        """Build metadata for assay pipeline."""
        # Create base metadata dictionary
        metadata = {
            "pipeline_name": "assays",
            "pipeline_version": "2.0.0",
            "entity_type": "assays",
            "sources_enabled": [name for name, source in self.config.sources.items() if source.enabled],
            "total_assays": len(data),
            "extraction_timestamp": pd.Timestamp.now().isoformat(),
            "config": self.config.model_dump() if hasattr(self.config, 'model_dump') else {},
        }
        
        return metadata
