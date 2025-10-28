"""Target ETL pipeline implementation using PipelineBase."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from library.clients.chembl import ChEMBLClient
from library.common.pipeline_base import PipelineBase
from library.target.config import TargetConfig
from library.target.normalize import TargetNormalizer
from library.target.quality import TargetQualityFilter
from library.target.validate import TargetValidator

logger = logging.getLogger(__name__)


class TargetPipeline(PipelineBase[TargetConfig]):
    """Target ETL pipeline using unified PipelineBase."""
    
    def __init__(self, config: TargetConfig) -> None:
        """Initialize target pipeline with configuration."""
        super().__init__(config)
        self.validator = TargetValidator(config.model_dump() if hasattr(config, 'model_dump') else {})
        self.normalizer = TargetNormalizer(config.model_dump() if hasattr(config, 'model_dump') else config)
        self.quality_filter = TargetQualityFilter(config.model_dump() if hasattr(config, 'model_dump') else {})
    
    def _setup_clients(self) -> None:
        """Initialize HTTP clients for target sources."""
        self.clients = {}
        
        # ChEMBL client
        if "chembl" in self.config.sources and self.config.sources["chembl"].get("enabled", False):
            self.clients["chembl"] = self._create_chembl_client()
    
    def _create_chembl_client(self) -> ChEMBLClient:
        """Create ChEMBL client."""
        from library.config import APIClientConfig, RateLimitSettings, RetrySettings
        
        source_config = self.config.sources["chembl"]
        timeout = source_config.get("http", {}).get("timeout_sec") or self.config.http.global_.timeout_sec
        timeout = max(timeout, 60.0)  # At least 60 seconds for ChEMBL
        
        headers = self._get_headers("chembl")
        headers.update(self.config.http.global_.headers)
        headers.update(source_config.get("http", {}).get("headers", {}))
        
        processed_headers = self._process_headers(headers)
        
        client_config = APIClientConfig(
            name="chembl",
            base_url=source_config.get("http", {}).get("base_url") or "https://www.ebi.ac.uk/chembl/api/data",
            timeout_sec=timeout,
            retries=RetrySettings(
                total=source_config.get("http", {}).get("retries", {}).get("total", 3),
                backoff_multiplier=source_config.get("http", {}).get("retries", {}).get("backoff_multiplier", 2.0),
                backoff_max=source_config.get("http", {}).get("retries", {}).get("backoff_max", 60.0),
            ),
            rate_limit=RateLimitSettings(
                max_calls=source_config.get("rate_limit", {}).get("max_calls", 10),
                period=source_config.get("rate_limit", {}).get("period", 1.0),
            ),
            headers=processed_headers,
            verify_ssl=True,
            follow_redirects=True,
        )
        
        return ChEMBLClient(client_config)
    
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
    
    def _get_entity_type(self) -> str:
        """Get entity type for this pipeline."""
        return "targets"
    
    def _create_qc_validator(self):
        """Create QC validator for target data."""
        from library.common.qc_profiles import QCValidator
        return QCValidator(self.config, "targets")
    
    def _create_postprocessor(self):
        """Create postprocessor for target data."""
        from library.common.postprocess_base import BasePostprocessor
        return BasePostprocessor(self.config, "targets")
    
    def _create_etl_writer(self):
        """Create ETL writer for target data."""
        from library.common.writer_base import ETLWriter
        return ETLWriter(self.config, "targets")
    
    def extract(self, input_data: pd.DataFrame) -> pd.DataFrame:
        """Extract target data from ChEMBL."""
        logger.info(f"Extracting target data for {len(input_data)} targets")
        
        # Validate input data
        validated_data = self.validator.validate_raw_data(input_data)
        
        # Apply limit if specified
        if self.config.runtime.limit is not None:
            validated_data = validated_data.head(self.config.runtime.limit)
            logger.info(f"Limited to {len(validated_data)} targets")
        
        # Check for duplicates
        duplicates = validated_data["target_chembl_id"].duplicated()
        if duplicates.any():
            raise ValueError("Duplicate target_chembl_id values detected")
        
        # Extract data from ChEMBL
        if "chembl" in self.clients:
            try:
                logger.info("Extracting data from ChEMBL")
                chembl_data = self._extract_from_chembl(validated_data)
                extracted_data = self._merge_chembl_data(validated_data, chembl_data)
            except Exception as e:
                logger.error(f"ChEMBL extraction failed: {e}")
                # Return input data with error information
                extracted_data = validated_data.copy()
                extracted_data["extraction_error"] = str(e)
        else:
            logger.warning("No ChEMBL client available")
            extracted_data = validated_data.copy()
        
        logger.info(f"Extraction completed. Records: {len(extracted_data)}")
        return extracted_data
    
    def _extract_from_chembl(self, input_data: pd.DataFrame) -> pd.DataFrame:
        """Extract target data from ChEMBL API."""
        chembl_client = self.clients["chembl"]
        target_records = []
        
        for idx, row in input_data.iterrows():
            target_chembl_id = row["target_chembl_id"]
            
            try:
                logger.debug(f"Fetching target data for {target_chembl_id}")
                target_data = chembl_client.fetch_by_target_id(target_chembl_id)
                
                if target_data and "error" not in target_data:
                    target_records.append(target_data)
                else:
                    logger.warning(f"Failed to fetch target {target_chembl_id}: {target_data.get('error', 'Unknown error')}")
                    # Add record with error information
                    error_record = {
                        "target_chembl_id": target_chembl_id,
                        "source_system": "ChEMBL",
                        "extraction_error": target_data.get("error", "Unknown error")
                    }
                    target_records.append(error_record)
                    
            except Exception as e:
                logger.error(f"Error fetching target {target_chembl_id}: {e}")
                # Add record with error information
                error_record = {
                    "target_chembl_id": target_chembl_id,
                    "source_system": "ChEMBL",
                    "extraction_error": str(e)
                }
                target_records.append(error_record)
        
        if target_records:
            return pd.DataFrame(target_records)
        else:
            logger.warning("No target data extracted from ChEMBL")
            return pd.DataFrame()
    
    def _merge_chembl_data(self, input_data: pd.DataFrame, chembl_data: pd.DataFrame) -> pd.DataFrame:
        """Merge ChEMBL data with input data."""
        if chembl_data.empty:
            logger.warning("No ChEMBL data to merge")
            return input_data
        
        # Merge on target_chembl_id
        merged_data = input_data.merge(
            chembl_data,
            on="target_chembl_id",
            how="left",
            suffixes=("", "_chembl")
        )
        
        logger.info(f"Merged ChEMBL data. Records: {len(merged_data)}")
        return merged_data
    
    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Normalize target data."""
        logger.info(f"Normalizing {len(raw_data)} target records")
        
        # Use the normalizer
        normalized_data = self.normalizer.normalize(raw_data)
        
        logger.info(f"Normalization completed. Records: {len(normalized_data)}")
        return normalized_data
    
    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate target data."""
        logger.info(f"Validating {len(data)} target records")
        
        # Use the validator
        validated_data = self.validator.validate_normalized_data(data)
        
        logger.info(f"Validation completed. Records: {len(validated_data)}")
        return validated_data
