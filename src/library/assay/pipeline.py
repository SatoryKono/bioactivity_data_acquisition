"""Refactored assay ETL pipeline using PipelineBase."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from library.assay.config import AssayConfig
from library.clients.chembl import ChEMBLClient
from library.common.pipeline_base import PipelineBase
from library.common.postprocess_base import AssayPostprocessor
from library.common.qc_profiles import QCValidator, get_qc_profile, get_qc_validator
from library.common.writer_base import ETLWriter, create_etl_writer

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
        """Extract data from ChEMBL with target enrichment."""
        logger.info(f"Extracting assay data from ChEMBL for {len(data)} assays")
        
        extracted_records = []
        chembl_client = self.clients["chembl"]
        
        # Track missing fields across all assays
        missing_fields = set()
        empty_fields = set()
        
        # Collect unique target IDs for enrichment
        target_ids = set()
        
        for _, row in data.iterrows():
            assay_id = row["assay_chembl_id"]
            try:
                assay_data = chembl_client.fetch_by_assay_id(assay_id)
                if assay_data:
                    extracted_records.append(assay_data)
                    
                    # Collect target_chembl_id for enrichment
                    target_chembl_id = assay_data.get("target_chembl_id")
                    if target_chembl_id:
                        target_ids.add(target_chembl_id)
                    
                    # Track which fields are missing or empty
                    for field, value in assay_data.items():
                        if field not in missing_fields and field not in empty_fields:
                            if value is None or value == "" or (isinstance(value, str) and value.strip() == ""):
                                empty_fields.add(field)
                else:
                    logger.warning(f"No data returned for assay {assay_id}")
            except Exception as e:
                logger.warning("Failed to fetch assay %s: %s", assay_id, e)
                continue
        
        if extracted_records:
            extracted_df = pd.DataFrame(extracted_records)
            logger.info(f"Successfully extracted {len(extracted_df)} assay records from ChEMBL")
            
            # Enrich with target data
            if target_ids:
                logger.info(f"Enriching {len(target_ids)} unique targets from /target endpoint")
                target_data = self._enrich_with_target_data(chembl_client, list(target_ids))
                if not target_data.empty:
                    # Merge target data into assay data
                    extracted_df = extracted_df.merge(
                        target_data,
                        on="target_chembl_id",
                        how="left",
                        suffixes=("", "_target")
                    )
                    logger.info("Enriched assay data with target information")
            
            # Log field analysis
            if empty_fields:
                logger.warning(f"Empty/missing fields in assay data: {sorted(empty_fields)}")
                logger.info("This may indicate incomplete data in ChEMBL or limited API response.")
            
            # Log information about unavailable fields
            unavailable_fields = [
                "bao_endpoint", "bao_assay_format", "bao_assay_type", "bao_assay_type_label",
                "bao_assay_type_uri", "bao_assay_format_uri", "bao_assay_format_label",
                "bao_endpoint_uri", "bao_endpoint_label", "variant_id", "is_variant",
                "variant_accession", "variant_sequence_accession", "variant_sequence_mutation",
                "variant_mutations", "variant_text", "variant_sequence_id", "variant_organism",
                "assay_parameters_json", "assay_format"
            ]
            logger.info(f"Fields unavailable in ChEMBL API: {unavailable_fields}")
            logger.info("These fields are documented as unavailable in ChEMBL API v33+")
            
            return extracted_df
        else:
            logger.warning("No assay data extracted from ChEMBL")
            return pd.DataFrame()
    
    def _enrich_with_target_data(self, chembl_client, target_ids: list[str]) -> pd.DataFrame:
        """Enrich assay data with target information."""
        target_records = []
        
        for target_id in target_ids:
            try:
                target_data = chembl_client.fetch_by_target_id(target_id)
                if target_data and "error" not in target_data:
                    target_records.append(target_data)
            except Exception as e:
                logger.warning("Failed to fetch target %s: %s", target_id, e)
                continue
        
        if target_records:
            return pd.DataFrame(target_records)
        else:
            logger.warning("No target data extracted for enrichment")
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
        
        # Add system metadata fields
        from library.common.metadata_fields import add_system_metadata_fields, create_chembl_client_from_config
        
        # Создаем ChEMBL клиент для получения версии
        config_dict = self.config.model_dump() if hasattr(self.config, 'model_dump') else {}
        chembl_client = create_chembl_client_from_config(config_dict)
        
        # Добавляем системные метаданные
        normalized_data = add_system_metadata_fields(normalized_data, config_dict, chembl_client)
        
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
    
    def _get_entity_type(self) -> str:
        """Get entity type for assay pipeline."""
        return "assays"
    
    def _create_qc_validator(self) -> QCValidator:
        """Create QC validator for assay pipeline."""
        profile = get_qc_profile("assays", "strict")
        return get_qc_validator("assays", profile)
    
    def _create_postprocessor(self) -> AssayPostprocessor:
        """Create postprocessor for assay pipeline."""
        return AssayPostprocessor(self.config)
    
    def _create_etl_writer(self) -> ETLWriter:
        """Create ETL writer for assay pipeline."""
        return create_etl_writer(self.config, "assays")
    
    def _build_metadata(
        self, 
        data: pd.DataFrame, 
        accepted_data: pd.DataFrame | None = None, 
        rejected_data: pd.DataFrame | None = None,
        correlation_analysis: dict[str, Any] | None = None,
        correlation_insights: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Build metadata for assay pipeline."""
        # Use MetadataBuilder to create proper PipelineMetadata
        from library.common.metadata import MetadataBuilder
        
        metadata_builder = MetadataBuilder(self.config, "assays")
        
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
