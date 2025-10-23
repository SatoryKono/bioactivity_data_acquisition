"""Refactored activity ETL pipeline using PipelineBase."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from library.activity.config import ActivityConfig
from library.activity.normalize import ActivityNormalizer
from library.activity.quality import ActivityQualityFilter
from library.activity.validate import ActivityValidator
from library.clients.chembl import ChEMBLClient
from library.common.pipeline_base import PipelineBase

logger = logging.getLogger(__name__)


class ActivityPipeline(PipelineBase[ActivityConfig]):
    """Activity ETL pipeline using unified PipelineBase."""
    
    def __init__(self, config: ActivityConfig) -> None:
        """Initialize activity pipeline with configuration."""
        super().__init__(config)
        self.validator = ActivityValidator(config.model_dump() if hasattr(config, 'model_dump') else {})
        self.normalizer = ActivityNormalizer(config.model_dump() if hasattr(config, 'model_dump') else {})
        self.quality_filter = ActivityQualityFilter(config.model_dump() if hasattr(config, 'model_dump') else {})
    
    def _setup_clients(self) -> None:
        """Initialize HTTP clients for activity sources."""
        self.clients = {}
        
        # ChEMBL client
        if "chembl" in self.config.sources and self.config.sources["chembl"].enabled:
            self.clients["chembl"] = self._create_chembl_client()
    
    def _create_chembl_client(self) -> ChEMBLClient:
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
    
    def extract(self, input_data: pd.DataFrame) -> pd.DataFrame:
        """Extract activity data from ChEMBL."""
        logger.info(f"Extracting activity data for {len(input_data)} activities")
        
        # Validate input data
        validated_data = self.validator.validate_raw_data(input_data)
        
        # Apply limit if specified
        if self.config.runtime.limit is not None:
            validated_data = validated_data.head(self.config.runtime.limit)
        
        # Check for duplicates
        duplicates = validated_data["activity_chembl_id"].duplicated()
        if duplicates.any():
            raise ValueError("Duplicate activity_chembl_id values detected")
        
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
        
        logger.info(f"Extracted data for {len(extracted_data)} activities")
        return extracted_data
    
    def _extract_from_chembl(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract data from ChEMBL."""
        from library.activity.client import ActivityChEMBLClient
        
        client = ActivityChEMBLClient(self.clients["chembl"].config)
        
        # Extract activity IDs from input data
        activity_ids = data["activity_chembl_id"].tolist()
        
        # Fetch activities from ChEMBL
        activities = []
        for activity in client.fetch_all_activities(activity_ids=activity_ids):
            activities.append(activity)
        
        # Convert to DataFrame
        if activities:
            return pd.DataFrame(activities)
        else:
            return pd.DataFrame()
    
    def _merge_chembl_data(self, base_data: pd.DataFrame, chembl_data: pd.DataFrame) -> pd.DataFrame:
        """Merge ChEMBL data into base data."""
        if chembl_data.empty:
            return base_data
        
        # Merge on activity_chembl_id
        merged = base_data.merge(
            chembl_data, 
            on="activity_chembl_id", 
            how="left", 
            suffixes=("", "_chembl")
        )
        
        # For fields that exist in both, prefer ChEMBL data
        chembl_fields = [
            "assay_chembl_id", "molecule_chembl_id", "target_chembl_id", "document_chembl_id",
            "published_type", "published_relation", "published_value", "published_units",
            "standard_type", "standard_relation", "standard_value", "standard_units", "standard_flag",
            "pchembl_value", "data_validity_comment", "activity_comment",
            "bao_endpoint", "bao_format", "bao_label"
        ]
        
        for field in chembl_fields:
            chembl_field = f"{field}_chembl"
            if chembl_field in merged.columns:
                # Use ChEMBL data where available, fallback to base data
                merged[field] = merged[chembl_field].fillna(merged[field])
                merged = merged.drop(columns=[chembl_field])
        
        return merged
    
    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Normalize activity data."""
        logger.info("Normalizing activity data")
        
        # Apply activity normalization
        normalized_data = self.normalizer.normalize_activities(raw_data)
        
        logger.info(f"Normalized {len(normalized_data)} activities")
        return normalized_data
    
    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate activity data."""
        logger.info("Validating activity data")
        
        # Validate normalized data
        validated_data = self.validator.validate_normalized_data(data)
        
        logger.info(f"Validated {len(validated_data)} activities")
        return validated_data
    
    def filter_quality(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Filter activities by quality."""
        logger.info("Filtering activities by quality")
        
        # Apply quality filters
        accepted_data, rejected_data = self.quality_filter.apply_moderate_quality_filter(data)
        
        logger.info(f"Quality filtering: {len(accepted_data)} accepted, {len(rejected_data)} rejected")
        return accepted_data, rejected_data
    
    def _get_entity_type(self) -> str:
        """Получить тип сущности для пайплайна."""
        return "activities"
    
    def _create_qc_validator(self) -> Any:
        """Создать QC валидатор для пайплайна."""
        from library.common.qc_profiles import ActivityQCValidator, QCProfile
        
        # Создаем базовый QC профиль для активностей
        qc_profile = QCProfile(
            name="activity_qc",
            description="Quality control profile for activities",
            rules=[]
        )
        
        return ActivityQCValidator(qc_profile)
    
    def _create_postprocessor(self) -> Any:
        """Создать постпроцессор для пайплайна."""
        from library.common.postprocess_base import ActivityPostprocessor
        return ActivityPostprocessor(self.config)
    
    def _create_etl_writer(self) -> Any:
        """Создать ETL writer для пайплайна."""
        from library.common.writer_base import create_etl_writer
        return create_etl_writer(self.config, "activities")
    
    def _build_metadata(
        self, 
        data: pd.DataFrame, 
        accepted_data: pd.DataFrame | None = None, 
        rejected_data: pd.DataFrame | None = None,
        correlation_analysis: dict[str, Any] | None = None,
        correlation_insights: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Build metadata for activity pipeline."""
        # Use MetadataBuilder to create proper PipelineMetadata
        from library.common.metadata import MetadataBuilder
        
        metadata_builder = MetadataBuilder(self.config, "activities")
        
        # Prepare additional metadata
        additional_metadata = {}
        if correlation_analysis is not None:
            additional_metadata["correlation_analysis"] = correlation_analysis
        if correlation_insights is not None:
            additional_metadata["correlation_insights"] = correlation_insights
        
        # Build proper metadata using MetadataBuilder
        metadata = metadata_builder.build_metadata(
            df=data,
            accepted_df=accepted_data if accepted_data is not None else data,
            rejected_df=rejected_data if rejected_data is not None else pd.DataFrame(),
            output_files={},  # Will be filled by writer
            additional_metadata=additional_metadata if additional_metadata else None
        )
        
        return metadata
