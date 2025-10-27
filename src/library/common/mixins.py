"""Mixins for common pipeline functionality.

This module provides reusable mixins that can be combined to create
specialized pipeline classes with shared functionality.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod

import pandas as pd

from .exceptions import ExtractionError, ValidationError
from .validation import ValidationStage, validate_entity_data

logger = logging.getLogger(__name__)


class ClientSetupMixin:
    """Mixin for setting up HTTP clients for different data sources."""

    def _setup_clients(self) -> None:
        """Initialize HTTP clients for data sources."""
        self.clients = {}

        # ChEMBL client setup
        if self._is_source_enabled("chembl"):
            self.clients["chembl"] = self._create_chembl_client()

        # PubChem client setup
        if self._is_source_enabled("pubchem"):
            self.clients["pubchem"] = self._create_pubchem_client()

        # Document-specific clients
        if hasattr(self, "_setup_document_clients") and callable(getattr(self, "_setup_document_clients", None)):
            try:
                self._setup_document_clients()  # type: ignore[attr-defined]
            except (AttributeError, TypeError):
                pass  # Method not implemented or not callable

    def _is_source_enabled(self, source_name: str) -> bool:
        """Check if a data source is enabled in configuration."""
        config = getattr(self, "config", None)
        if not config or not hasattr(config, "sources"):
            return False
        return source_name in config.sources and config.sources[source_name].enabled

    def _create_chembl_client(self):
        """Create ChEMBL client."""
        # from library.clients.chembl import ChEMBLClient
        # from library.config import APIClientConfig, RateLimitSettings, RetrySettings

        # source_config = getattr(self, 'config', {}).get('sources', {}).get("chembl", {})
        # Simplified client creation - will be implemented when config is available
        return None

    def _create_pubchem_client(self):
        """Create PubChem client."""
        # from library.clients.pubchem import PubChemClient
        # from library.config import APIClientConfig, RateLimitSettings, RetrySettings

        # source_config = getattr(self, 'config', {}).get('sources', {}).get("pubchem", {})
        # Simplified client creation - will be implemented when config is available
        return None


class DataExtractionMixin(ABC):
    """Mixin for extracting data from multiple sources."""

    def extract(self, input_data: pd.DataFrame) -> pd.DataFrame:
        """Extract data from multiple sources."""
        logger.info(f"Extracting {self._get_entity_type()} data for {len(input_data)} records")

        # Apply limit if specified
        if self._has_runtime_limit():
            limit = getattr(getattr(self, "config", {}), "runtime", {}).get("limit", None)
            if limit:
                input_data = input_data.head(limit)

        # Check for duplicates
        self._validate_no_duplicates(input_data)

        # Extract data from each enabled source
        extracted_data = input_data.copy()

        clients = getattr(self, "clients", {})
        for source_name, client in clients.items():
            try:
                logger.info(f"Extracting data from {source_name}")
                source_data = self._extract_from_source(client, source_name, extracted_data)
                extracted_data = self._merge_source_data(extracted_data, source_data, source_name)

                if not source_data.empty:
                    logger.info(f"Successfully extracted {len(source_data)} records from {source_name}")
                else:
                    logger.warning(f"No data extracted from {source_name}")

            except Exception as e:
                logger.error(f"Failed to extract from {source_name}: {e}", exc_info=True)
                if not self._allow_incomplete_sources():
                    raise ExtractionError(f"Failed to extract from {source_name}", data_source=source_name, cause=e) from e

                # Create error dataframe for graceful degradation
                error_data = self._create_error_dataframe(extracted_data, source_name, str(e))
                extracted_data = self._merge_source_data(extracted_data, error_data, source_name)

        logger.info(f"Extracted data for {len(extracted_data)} {self._get_entity_type()} records")
        return extracted_data

    @abstractmethod
    def _extract_from_source(self, client, source_name: str, data: pd.DataFrame) -> pd.DataFrame:
        """Extract data from a specific source."""
        pass

    @abstractmethod
    def _merge_source_data(self, base_data: pd.DataFrame, source_data: pd.DataFrame, source_name: str) -> pd.DataFrame:
        """Merge data from a source into base data."""
        pass

    def _validate_no_duplicates(self, data: pd.DataFrame) -> None:
        """Validate that there are no duplicate records."""
        key_column = self._get_key_column()
        if key_column in data.columns:
            duplicates = data[key_column].duplicated()
            if duplicates.any():
                raise ValueError(f"Duplicate {key_column} values detected")

    def _create_error_dataframe(self, base_data: pd.DataFrame, source_name: str, error_message: str) -> pd.DataFrame:
        """Create error dataframe for graceful degradation."""
        error_data = base_data.copy()
        error_data[f"{source_name}_error"] = error_message
        error_data[f"{source_name}_extracted"] = False
        return error_data

    def _has_runtime_limit(self) -> bool:
        """Check if runtime limit is specified."""
        config = getattr(self, "config", {})
        runtime = getattr(config, "runtime", {})
        return hasattr(runtime, "limit") and getattr(runtime, "limit", None) is not None

    def _allow_incomplete_sources(self) -> bool:
        """Check if incomplete sources are allowed."""
        config = getattr(self, "config", {})
        runtime = getattr(config, "runtime", {})
        return hasattr(runtime, "allow_incomplete_sources") and getattr(runtime, "allow_incomplete_sources", False)

    @abstractmethod
    def _get_entity_type(self) -> str:
        """Get the entity type name."""
        pass

    @abstractmethod
    def _get_key_column(self) -> str:
        """Get the primary key column name."""
        pass


class ValidationMixin(ABC):
    """Mixin for data validation."""

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate data using unified validation system."""
        logger.info(f"Validating {self._get_entity_type()} data")

        # Use unified validation system
        result = validate_entity_data(self._get_entity_type(), data, ValidationStage.NORMALIZED)

        if not result.is_valid:
            error_messages = [str(error) for error in result.errors]
            raise ValidationError(f"Validation failed for {self._get_entity_type()} data", validation_errors=error_messages)

        logger.info(f"Validated {len(data)} {self._get_entity_type()} records")
        return data

    @abstractmethod
    def _get_entity_type(self) -> str:
        """Get the entity type name."""
        pass


class NormalizationMixin(ABC):
    """Mixin for data normalization."""

    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Normalize data using entity-specific normalizer."""
        logger.info(f"Normalizing {self._get_entity_type()} data")

        if hasattr(self, "normalizer") and getattr(self, "normalizer", None):
            normalized_data = self._apply_normalization(raw_data)
        else:
            # Fallback to basic normalization
            normalized_data = self._apply_basic_normalization(raw_data)

        logger.info(f"Normalized {len(normalized_data)} {self._get_entity_type()} records")
        return normalized_data

    def _apply_normalization(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Apply entity-specific normalization."""
        normalizer = getattr(self, "normalizer", None)
        if normalizer and hasattr(normalizer, f"normalize_{self._get_entity_type()}s"):
            method = getattr(normalizer, f"normalize_{self._get_entity_type()}s")
            return method(raw_data)
        else:
            return self._apply_basic_normalization(raw_data)

    def _apply_basic_normalization(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Apply basic normalization."""
        normalized_data = raw_data.copy()

        # Basic string normalization
        for col in normalized_data.select_dtypes(include=["object"]).columns:
            if normalized_data[col].dtype == "object":
                normalized_data[col] = normalized_data[col].astype(str).str.strip()
                # Replace string representations of empty values with pd.NA
                normalized_data[col] = normalized_data[col].replace(["None", "nan", "NaN", "none", "NULL", "null"], pd.NA)
                normalized_data[col] = normalized_data[col].replace("", pd.NA)

        return normalized_data

    @abstractmethod
    def _get_entity_type(self) -> str:
        """Get the entity type name."""
        pass


class QualityFilteringMixin(ABC):
    """Mixin for quality filtering."""

    def filter_quality(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Filter data by quality criteria."""
        logger.info(f"Filtering {self._get_entity_type()} data by quality")

        if hasattr(self, "quality_filter") and getattr(self, "quality_filter", None):
            accepted_data, rejected_data = self._apply_quality_filter(data)
        else:
            # Fallback to basic quality filtering
            accepted_data, rejected_data = self._apply_basic_quality_filter(data)

        logger.info(f"Quality filtering: {len(accepted_data)} accepted, {len(rejected_data)} rejected")
        return accepted_data, rejected_data

    def _apply_quality_filter(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Apply entity-specific quality filter."""
        quality_filter = getattr(self, "quality_filter", None)
        if quality_filter and hasattr(quality_filter, f"apply_{self._get_entity_type()}_quality_filter"):
            method = getattr(quality_filter, f"apply_{self._get_entity_type()}_quality_filter")
            return method(data)
        elif quality_filter and hasattr(quality_filter, "apply_moderate_quality_filter"):
            return quality_filter.apply_moderate_quality_filter(data)
        else:
            return self._apply_basic_quality_filter(data)

    def _apply_basic_quality_filter(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Apply basic quality filtering."""
        # Basic quality criteria: no completely empty rows
        key_column = self._get_key_column()
        if key_column in data.columns:
            accepted_mask = data[key_column].notna()
            accepted_data = data[accepted_mask].copy()
            rejected_data = data[~accepted_mask].copy()
        else:
            # If no key column, accept all data
            accepted_data = data.copy()
            rejected_data = pd.DataFrame()

        return accepted_data, rejected_data

    @abstractmethod
    def _get_entity_type(self) -> str:
        """Get the entity type name."""
        pass

    @abstractmethod
    def _get_key_column(self) -> str:
        """Get the primary key column name."""
        pass


class DataMergingMixin(ABC):
    """Mixin for merging data from different sources."""

    def _merge_chembl_data(self, base_data: pd.DataFrame, chembl_data: pd.DataFrame) -> pd.DataFrame:
        """Merge ChEMBL data into base data."""
        if chembl_data.empty:
            return base_data

        # Merge on the primary key
        key_column = self._get_key_column()
        merged = base_data.merge(chembl_data, on=key_column, how="left", suffixes=("", "_chembl"))

        # Merge specific fields from ChEMBL
        chembl_fields = self._get_chembl_fields()
        for field in chembl_fields:
            chembl_field = f"{field}_chembl"
            if chembl_field in merged.columns:
                before_count = merged[field].notna().sum() if field in merged.columns else 0
                merged[field] = merged[chembl_field].fillna(merged[field])
                after_count = merged[field].notna().sum()
                merged = merged.drop(columns=[chembl_field])

                logger.debug(f"Field {field}: {before_count} -> {after_count} non-null values after merge")

        return merged

    def _merge_pubchem_data(self, base_data: pd.DataFrame, pubchem_data: pd.DataFrame) -> pd.DataFrame:
        """Merge PubChem data into base data."""
        if pubchem_data.empty:
            return base_data

        # Merge on the primary key
        key_column = self._get_key_column()
        merged = base_data.merge(pubchem_data, on=key_column, how="left", suffixes=("", "_pubchem"))

        # Merge specific fields from PubChem
        pubchem_fields = self._get_pubchem_fields()
        for field in pubchem_fields:
            pubchem_field = f"{field}_pubchem"
            if pubchem_field in merged.columns:
                before_count = merged[field].notna().sum() if field in merged.columns else 0
                merged[field] = merged[pubchem_field].fillna(merged[field])
                after_count = merged[field].notna().sum()
                merged = merged.drop(columns=[pubchem_field])

                logger.debug(f"Field {field}: {before_count} -> {after_count} non-null values after merge")

        return merged

    def _get_chembl_fields(self) -> list[str]:
        """Get list of fields to merge from ChEMBL."""
        return []

    def _get_pubchem_fields(self) -> list[str]:
        """Get list of fields to merge from PubChem."""
        return []

    @abstractmethod
    def _get_key_column(self) -> str:
        """Get the primary key column name."""
        pass


class EntityTypeMixin(ABC):
    """Mixin for entity type identification."""

    @abstractmethod
    def _get_entity_type(self) -> str:
        """Get the entity type name."""
        pass

    @abstractmethod
    def _get_key_column(self) -> str:
        """Get the primary key column name."""
        pass


# Composite mixins for common combinations
class StandardPipelineMixin(ClientSetupMixin, DataExtractionMixin, ValidationMixin, NormalizationMixin, QualityFilteringMixin, DataMergingMixin, EntityTypeMixin):
    """Standard mixin combining all common pipeline functionality."""

    pass


class ChEMBLOnlyMixin(ClientSetupMixin, DataExtractionMixin, ValidationMixin, NormalizationMixin, QualityFilteringMixin, EntityTypeMixin):
    """Mixin for pipelines that only use ChEMBL data."""

    def _setup_clients(self) -> None:
        """Initialize only ChEMBL client."""
        self.clients = {}

        if self._is_source_enabled("chembl"):
            self.clients["chembl"] = self._create_chembl_client()

    def _extract_from_source(self, client, source_name: str, data: pd.DataFrame) -> pd.DataFrame:
        """Extract data from ChEMBL."""
        if source_name == "chembl":
            return self._extract_from_chembl(data)
        else:
            return pd.DataFrame()

    def _merge_source_data(self, base_data: pd.DataFrame, source_data: pd.DataFrame, source_name: str) -> pd.DataFrame:
        """Merge ChEMBL data."""
        if source_name == "chembl":
            return self._merge_chembl_data(base_data, source_data)
        else:
            return base_data

    def _merge_chembl_data(self, base_data: pd.DataFrame, chembl_data: pd.DataFrame) -> pd.DataFrame:
        """Merge ChEMBL data into base data."""
        if chembl_data.empty:
            return base_data

        # Merge on the primary key
        key_column = self._get_key_column()
        merged = base_data.merge(chembl_data, on=key_column, how="left", suffixes=("", "_chembl"))

        # Merge specific fields from ChEMBL
        chembl_fields = self._get_chembl_fields()
        for field in chembl_fields:
            chembl_field = f"{field}_chembl"
            if chembl_field in merged.columns:
                before_count = merged[field].notna().sum() if field in merged.columns else 0
                merged[field] = merged[chembl_field].fillna(merged[field])
                after_count = merged[field].notna().sum()
                merged = merged.drop(columns=[chembl_field])

                logger.debug(f"Field {field}: {before_count} -> {after_count} non-null values after merge")

        return merged

    @abstractmethod
    def _extract_from_chembl(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract data from ChEMBL."""
        pass

    def _get_chembl_fields(self) -> list[str]:
        """Get list of fields to merge from ChEMBL."""
        return []


class MultiSourceMixin(ClientSetupMixin, DataExtractionMixin, ValidationMixin, NormalizationMixin, QualityFilteringMixin, DataMergingMixin, EntityTypeMixin):
    """Mixin for pipelines that use multiple data sources."""

    pass
