"""Refactored testitem ETL pipeline using PipelineBase."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from library.clients.chembl import ChEMBLClient
from library.common.pipeline_base import PipelineBase
from library.testitem.config import TestitemConfig
from library.testitem.normalize import TestitemNormalizer
from library.testitem.quality import TestitemQualityFilter
from library.testitem.validate import TestitemValidator

logger = logging.getLogger(__name__)


class TestitemPipeline(PipelineBase[TestitemConfig]):
    """Testitem ETL pipeline using unified PipelineBase."""
    
    def __init__(self, config: TestitemConfig) -> None:
        """Initialize testitem pipeline with configuration."""
        super().__init__(config)
        self.validator = TestitemValidator(config.model_dump() if hasattr(config, 'model_dump') else {})
        self.normalizer = TestitemNormalizer(config.model_dump() if hasattr(config, 'model_dump') else {})
        self.quality_filter = TestitemQualityFilter(config.model_dump() if hasattr(config, 'model_dump') else {})
    
    def _setup_clients(self) -> None:
        """Initialize HTTP clients for testitem sources."""
        self.clients = {}
        
        # ChEMBL client
        if self.config.sources.get("chembl", {}).get("enabled", False):
            self.clients["chembl"] = self._create_chembl_client()
        
        # PubChem client
        if self.config.sources.get("pubchem", {}).get("enabled", False):
            self.clients["pubchem"] = self._create_pubchem_client()
    
    def _create_chembl_client(self) -> ChEMBLClient:
        """Create ChEMBL client."""
        from library.config import RateLimitSettings, RetrySettings
        
        source_config = self.config.sources["chembl"]
        timeout = source_config.http.timeout_sec or self.config.http.global_.timeout_sec
        timeout = max(timeout, 60.0)  # At least 60 seconds for ChEMBL
        
        headers = self._get_headers("chembl")
        headers.update(self.config.http.global_.headers)
        headers.update(source_config.http.headers)
        
        processed_headers = self._process_headers(headers)
        
        from library.config import APIClientConfig
        
        client_config = APIClientConfig(
            name="chembl",
            base_url=source_config.http.base_url,
            timeout_sec=timeout,
            retries=RetrySettings(
                total=source_config.http.retries.total,
                backoff_multiplier=source_config.http.retries.backoff_multiplier,
                backoff_max=source_config.http.retries.backoff_max,
            ),
            rate_limit=RateLimitSettings(
                max_calls=source_config.rate_limit.max_calls,
                period=source_config.rate_limit.period,
            ),
            headers=processed_headers,
            verify_ssl=source_config.http.verify_ssl,
            follow_redirects=source_config.http.follow_redirects,
        )
        
        return ChEMBLClient(client_config)
    
    def _create_pubchem_client(self) -> Any:
        """Create PubChem client."""
        # This would create the actual PubChem client
        # For now, return a placeholder
        return None
    
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
        """Extract testitem data from multiple sources."""
        logger.info(f"Extracting testitem data for {len(input_data)} testitems")
        
        # Validate input data
        validated_data = self.validator.validate_raw(input_data)
        
        # Apply limit if specified
        if self.config.runtime.limit is not None:
            validated_data = validated_data.head(self.config.runtime.limit)
        
        # Check for duplicates
        duplicates = validated_data["molecule_chembl_id"].duplicated()
        if duplicates.any():
            raise ValueError("Duplicate molecule_chembl_id values detected")
        
        # Extract data from each enabled source
        extracted_data = validated_data.copy()
        
        # ChEMBL extraction
        if "chembl" in self.clients:
            try:
                logger.info("Extracting data from ChEMBL")
                chembl_data = self._extract_from_chembl(extracted_data)
                extracted_data = self._merge_chembl_data(extracted_data, chembl_data)
            except Exception as e:
                logger.error(f"Failed to extract from ChEMBL: {e}")
                if not self.config.runtime.allow_incomplete_sources:
                    raise
        
        # PubChem extraction
        if "pubchem" in self.clients:
            try:
                logger.info("Extracting data from PubChem")
                pubchem_data = self._extract_from_pubchem(extracted_data)
                extracted_data = self._merge_pubchem_data(extracted_data, pubchem_data)
            except Exception as e:
                logger.error(f"Failed to extract from PubChem: {e}")
                if not self.config.runtime.allow_incomplete_sources:
                    raise
        
        logger.info(f"Extracted data for {len(extracted_data)} testitems")
        return extracted_data
    
    def _extract_from_chembl(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract data from ChEMBL."""
        # This would contain the actual ChEMBL extraction logic
        # For now, return empty DataFrame as placeholder
        return pd.DataFrame()
    
    def _extract_from_pubchem(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract data from PubChem."""
        # This would contain the actual PubChem extraction logic
        # For now, return empty DataFrame as placeholder
        return pd.DataFrame()
    
    def _merge_chembl_data(self, base_data: pd.DataFrame, chembl_data: pd.DataFrame) -> pd.DataFrame:
        """Merge ChEMBL data into base data."""
        # This would contain the actual merging logic
        # For now, return base data as placeholder
        return base_data
    
    def _merge_pubchem_data(self, base_data: pd.DataFrame, pubchem_data: pd.DataFrame) -> pd.DataFrame:
        """Merge PubChem data into base data."""
        # This would contain the actual merging logic
        # For now, return base data as placeholder
        return base_data
    
    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Normalize testitem data."""
        logger.info("Normalizing testitem data")
        
        # Apply testitem normalization
        normalized_data = self.normalizer.normalize_testitems(raw_data)
        
        logger.info(f"Normalized {len(normalized_data)} testitems")
        return normalized_data
    
    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate testitem data."""
        logger.info("Validating testitem data")
        
        # Validate normalized data
        validated_data = self.validator.validate_normalized(data)
        
        logger.info(f"Validated {len(validated_data)} testitems")
        return validated_data
    
    def filter_quality(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Filter testitems by quality."""
        logger.info("Filtering testitems by quality")
        
        # Apply quality filters
        accepted_data, rejected_data = self.quality_filter.apply_moderate_quality_filter(data)
        
        logger.info(f"Quality filtering: {len(accepted_data)} accepted, {len(rejected_data)} rejected")
        return accepted_data, rejected_data
    
    def _get_entity_type(self) -> str:
        """Получить тип сущности для пайплайна."""
        return "testitems"
    
    def _create_qc_validator(self) -> Any:
        """Создать QC валидатор для пайплайна."""
        from library.common.qc_profiles import TestitemQCValidator, QCProfile
        
        # Создаем базовый QC профиль для теститемов
        qc_profile = QCProfile(
            name="testitem_qc",
            description="Quality control profile for testitems",
            rules=[]
        )
        
        return TestitemQCValidator(qc_profile)
    
    def _create_postprocessor(self) -> Any:
        """Создать постпроцессор для пайплайна."""
        from library.common.postprocess_base import TestitemPostprocessor
        return TestitemPostprocessor(self.config)
    
    def _create_etl_writer(self) -> Any:
        """Создать ETL writer для пайплайна."""
        from library.common.writer_base import create_etl_writer
        return create_etl_writer(self.config, "testitems")
    
    def _build_metadata(self, data: pd.DataFrame) -> dict[str, Any]:
        """Build metadata for testitem pipeline."""
        # Create base metadata dictionary
        metadata = {
            "pipeline_name": "testitems",
            "pipeline_version": "2.0.0",
            "entity_type": "testitems",
            "sources_enabled": [name for name, source in self.config.sources.items() if source.get("enabled", False)],
            "total_testitems": len(data),
            "extraction_timestamp": pd.Timestamp.now().isoformat(),
            "config": self.config.model_dump() if hasattr(self.config, 'model_dump') else {},
        }
        
        return metadata


# Import required modules - removed circular import
