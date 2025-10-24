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
        
        # Collect unique target IDs and assay class IDs for enrichment
        target_ids = set()
        assay_class_ids = set()
        
        for _, row in data.iterrows():
            assay_id = row["assay_chembl_id"]
            try:
                assay_data = chembl_client.fetch_by_assay_id(assay_id)
                if assay_data:
                    # Expand assay parameters
                    assay_data = self._expand_assay_parameters(assay_data)
                    
                    # Expand variant sequence
                    assay_data = self._expand_variant_sequence(assay_data)
                    
                    extracted_records.append(assay_data)
                    
                    # Collect target_chembl_id for enrichment
                    target_chembl_id = assay_data.get("target_chembl_id")
                    if target_chembl_id:
                        target_ids.add(target_chembl_id)
                    
                    # Collect assay_class_ids from assay_classifications
                    classifications = assay_data.get("assay_classifications")
                    if classifications:
                        try:
                            import json
                            class_data = json.loads(classifications)
                            if isinstance(class_data, list):
                                for class_item in class_data:
                                    if isinstance(class_item, dict) and "assay_class_id" in class_item:
                                        assay_class_ids.add(class_item["assay_class_id"])
                        except (json.JSONDecodeError, TypeError) as e:
                            logger.debug(f"Failed to parse assay_classifications for {assay_id}: {e}")
                    
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
            
            # Enrich with assay class data
            if assay_class_ids:
                logger.info(f"Enriching {len(assay_class_ids)} unique assay classes from /assay_class endpoint")
                class_data = self._enrich_with_assay_classes(chembl_client, list(assay_class_ids))
                if not class_data.empty:
                    # Merge class data into assay data
                    # Note: We need to handle the many-to-many relationship between assays and classes
                    # For now, we'll merge on the first assay_class_id found in classifications
                    extracted_df = self._merge_assay_class_data(extracted_df, class_data)
                    logger.info("Enriched assay data with assay class information")
            
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
    
    def _expand_assay_parameters(self, assay_data: dict[str, Any]) -> dict[str, Any]:
        """Expand first assay_parameter to flat fields with assay_param_ prefix."""
        params = assay_data.get("assay_parameters")
        if params and isinstance(params, list) and len(params) > 0:
            first_param = params[0]
            assay_data["assay_param_type"] = first_param.get("type")
            assay_data["assay_param_relation"] = first_param.get("relation")
            assay_data["assay_param_value"] = first_param.get("value")
            assay_data["assay_param_units"] = first_param.get("units")
            assay_data["assay_param_text_value"] = first_param.get("text_value")
            assay_data["assay_param_standard_type"] = first_param.get("standard_type")
            assay_data["assay_param_standard_value"] = first_param.get("standard_value")
            assay_data["assay_param_standard_units"] = first_param.get("standard_units")
        else:
            # Set NULL for all fields
            for field in ["type", "relation", "value", "units", "text_value", 
                          "standard_type", "standard_value", "standard_units"]:
                assay_data[f"assay_param_{field}"] = None
        return assay_data
    
    def _expand_variant_sequence(self, assay_data: dict[str, Any]) -> dict[str, Any]:
        """Expand variant_sequence object to flat fields with variant_ prefix."""
        variant = assay_data.get("variant_sequence")
        if variant and isinstance(variant, dict):
            assay_data["variant_id"] = variant.get("variant_id")
            assay_data["variant_base_accession"] = variant.get("accession") or variant.get("base_accession")
            assay_data["variant_mutation"] = variant.get("mutation")
            assay_data["variant_sequence"] = variant.get("sequence")
            assay_data["variant_accession_reported"] = variant.get("accession")
        else:
            for field in ["variant_id", "variant_base_accession", "variant_mutation", 
                          "variant_sequence", "variant_accession_reported"]:
                assay_data[field] = None
        return assay_data
    
    def _enrich_with_assay_classes(self, chembl_client, assay_class_ids: list[int]) -> pd.DataFrame:
        """Fetch assay class data for given IDs."""
        class_records = []
        for class_id in assay_class_ids:
            try:
                class_data = chembl_client.fetch_assay_class(class_id)
                if class_data and "error" not in class_data:
                    class_records.append(class_data)
            except Exception as e:
                logger.warning(f"Failed to fetch assay_class {class_id}: {e}")
        return pd.DataFrame(class_records) if class_records else pd.DataFrame()
    
    def _merge_assay_class_data(self, assay_df: pd.DataFrame, class_df: pd.DataFrame) -> pd.DataFrame:
        """Merge assay class data into assay DataFrame."""
        if class_df.empty:
            return assay_df
        
        # Create a mapping from assay_class_id to class data
        class_dict = class_df.set_index('assay_class_id').to_dict('index')
        
        # For each assay, find the first assay_class_id from classifications and merge its data
        for idx, row in assay_df.iterrows():
            classifications = row.get("assay_classifications")
            if classifications:
                try:
                    import json
                    class_data = json.loads(classifications)
                    if isinstance(class_data, list) and len(class_data) > 0:
                        # Take the first classification
                        first_class = class_data[0]
                        if isinstance(first_class, dict) and "assay_class_id" in first_class:
                            class_id = first_class["assay_class_id"]
                            if class_id in class_dict:
                                # Merge class data into this row
                                class_info = class_dict[class_id]
                                for key, value in class_info.items():
                                    if key != 'assay_class_id':  # Don't overwrite the ID
                                        assay_df.at[idx, key] = value
                except (json.JSONDecodeError, TypeError, KeyError) as e:
                    logger.debug(f"Failed to merge assay class data for row {idx}: {e}")
                    continue
        
        return assay_df
    
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
        
        # Apply assay-specific normalization
        from library.normalize.assay import normalize_assay_dataframe
        normalized_data = normalize_assay_dataframe(raw_data)
        
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
