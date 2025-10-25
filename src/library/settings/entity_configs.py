"""Entity-specific configuration classes using the base configuration system.

This module provides specialized configuration classes for different entity types
that extend the base configuration system with entity-specific settings.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field, field_validator, model_validator

from .base import (
    BaseConfig,
    IOSection,
    RuntimeSection,
    SourceConfigSection,
)


class ActivitySourceSettings(SourceConfigSection):
    """Configuration for activity data sources."""

    # Activity-specific settings
    include_inactive: bool = Field(default=False, description="Include inactive activities")
    min_confidence_score: float = Field(default=0.0, ge=0.0, le=1.0, description="Minimum confidence score")
    activity_types: list[str] = Field(default_factory=list, description="Activity types to include")

    @field_validator("activity_types")
    @classmethod
    def _validate_activity_types(cls, v: list[str]) -> list[str]:
        """Validate activity types."""
        valid_types = ["IC50", "EC50", "Ki", "Kd", "AC50", "Potency"]
        for activity_type in v:
            if activity_type not in valid_types:
                raise ValueError(f"Invalid activity type: {activity_type}. Must be one of {valid_types}")
        return v


class ActivityIOSettings(IOSection):
    """I/O settings for activity pipeline."""

    input_file: str = Field(default="activities.csv", description="Input CSV file name")
    output_file: str = Field(default="activities_normalized.parquet", description="Output file name")
    qc_report_file: str = Field(default="activity_qc_report.json", description="QC report file name")


class ActivityRuntimeSettings(RuntimeSection):
    """Runtime settings for activity pipeline."""

    # Activity-specific runtime settings
    validate_activity_ids: bool = Field(default=True, description="Validate activity ChEMBL IDs")
    normalize_units: bool = Field(default=True, description="Normalize activity units")
    filter_by_confidence: bool = Field(default=True, description="Filter by confidence score")


class ActivityConfig(BaseConfig):
    """Configuration for activity ETL pipeline."""

    # Activity-specific sections
    sources: dict[str, ActivitySourceSettings] = Field(default_factory=dict)
    io: ActivityIOSettings = Field(default_factory=ActivityIOSettings)
    runtime: ActivityRuntimeSettings = Field(default_factory=ActivityRuntimeSettings)

    @model_validator(mode="after")
    def _validate_activity_config(self) -> ActivityConfig:
        """Validate activity-specific configuration."""
        # Ensure at least one source is enabled
        enabled_sources = [name for name, source in self.sources.items() if source.enabled]
        if not enabled_sources:
            raise ValueError("At least one data source must be enabled")

        return self


class AssaySourceSettings(SourceConfigSection):
    """Configuration for assay data sources."""

    # Assay-specific settings
    include_inactive: bool = Field(default=False, description="Include inactive assays")
    assay_types: list[str] = Field(default_factory=list, description="Assay types to include")
    organism_filter: list[str] = Field(default_factory=list, description="Organisms to include")

    @field_validator("assay_types")
    @classmethod
    def _validate_assay_types(cls, v: list[str]) -> list[str]:
        """Validate assay types."""
        valid_types = ["B", "F", "A", "T", "P", "U"]
        for assay_type in v:
            if assay_type not in valid_types:
                raise ValueError(f"Invalid assay type: {assay_type}. Must be one of {valid_types}")
        return v


class AssayIOSettings(IOSection):
    """I/O settings for assay pipeline."""

    input_file: str = Field(default="assays.csv", description="Input CSV file name")
    output_file: str = Field(default="assays_normalized.parquet", description="Output file name")
    qc_report_file: str = Field(default="assay_qc_report.json", description="QC report file name")


class AssayRuntimeSettings(RuntimeSection):
    """Runtime settings for assay pipeline."""

    # Assay-specific runtime settings
    enrich_with_targets: bool = Field(default=True, description="Enrich assays with target data")
    expand_parameters: bool = Field(default=True, description="Expand assay parameters")
    include_variants: bool = Field(default=False, description="Include variant sequences")


class AssayConfig(BaseConfig):
    """Configuration for assay ETL pipeline."""

    # Assay-specific sections
    sources: dict[str, AssaySourceSettings] = Field(default_factory=dict)
    io: AssayIOSettings = Field(default_factory=AssayIOSettings)
    runtime: AssayRuntimeSettings = Field(default_factory=AssayRuntimeSettings)

    @model_validator(mode="after")
    def _validate_assay_config(self) -> AssayConfig:
        """Validate assay-specific configuration."""
        # Ensure at least one source is enabled
        enabled_sources = [name for name, source in self.sources.items() if source.enabled]
        if not enabled_sources:
            raise ValueError("At least one data source must be enabled")

        return self


class DocumentSourceSettings(SourceConfigSection):
    """Configuration for document data sources."""

    # Document-specific settings
    include_abstracts: bool = Field(default=True, description="Include document abstracts")
    include_authors: bool = Field(default=True, description="Include author information")
    include_citations: bool = Field(default=False, description="Include citation data")
    year_range: tuple[int, int] | None = Field(default=None, description="Year range filter")

    @field_validator("year_range")
    @classmethod
    def _validate_year_range(cls, v: tuple[int, int] | None) -> tuple[int, int] | None:
        """Validate year range."""
        if v is not None:
            start_year, end_year = v
            if start_year > end_year:
                raise ValueError("Start year must be less than or equal to end year")
            if start_year < 1900 or end_year > 2030:
                raise ValueError("Year range must be between 1900 and 2030")
        return v


class DocumentIOSettings(IOSection):
    """I/O settings for document pipeline."""

    input_file: str = Field(default="documents.csv", description="Input CSV file name")
    output_file: str = Field(default="documents_normalized.parquet", description="Output file name")
    qc_report_file: str = Field(default="document_qc_report.json", description="QC report file name")
    citation_file: str = Field(default="citations.json", description="Citation data file name")


class DocumentRuntimeSettings(RuntimeSection):
    """Runtime settings for document pipeline."""

    # Document-specific runtime settings
    normalize_journals: bool = Field(default=True, description="Normalize journal names")
    extract_doi: bool = Field(default=True, description="Extract DOI information")
    validate_pmid: bool = Field(default=True, description="Validate PMID format")
    merge_duplicates: bool = Field(default=True, description="Merge duplicate documents")


class DocumentConfig(BaseConfig):
    """Configuration for document ETL pipeline."""

    # Document-specific sections
    sources: dict[str, DocumentSourceSettings] = Field(default_factory=dict)
    io: DocumentIOSettings = Field(default_factory=DocumentIOSettings)
    runtime: DocumentRuntimeSettings = Field(default_factory=DocumentRuntimeSettings)

    @model_validator(mode="after")
    def _validate_document_config(self) -> DocumentConfig:
        """Validate document-specific configuration."""
        # Ensure at least one source is enabled
        enabled_sources = [name for name, source in self.sources.items() if source.enabled]
        if not enabled_sources:
            raise ValueError("At least one data source must be enabled")

        return self


class TargetSourceSettings(SourceConfigSection):
    """Configuration for target data sources."""

    # Target-specific settings
    include_inactive: bool = Field(default=False, description="Include inactive targets")
    target_types: list[str] = Field(default_factory=list, description="Target types to include")
    organism_filter: list[str] = Field(default_factory=list, description="Organisms to include")

    @field_validator("target_types")
    @classmethod
    def _validate_target_types(cls, v: list[str]) -> list[str]:
        """Validate target types."""
        valid_types = ["SINGLE PROTEIN", "PROTEIN COMPLEX", "PROTEIN FAMILY", "PROTEIN-PROTEIN INTERACTION"]
        for target_type in v:
            if target_type not in valid_types:
                raise ValueError(f"Invalid target type: {target_type}. Must be one of {valid_types}")
        return v


class TargetIOSettings(IOSection):
    """I/O settings for target pipeline."""

    input_file: str = Field(default="targets.csv", description="Input CSV file name")
    output_file: str = Field(default="targets_normalized.parquet", description="Output file name")
    qc_report_file: str = Field(default="target_qc_report.json", description="QC report file name")


class TargetRuntimeSettings(RuntimeSection):
    """Runtime settings for target pipeline."""

    # Target-specific runtime settings
    expand_components: bool = Field(default=True, description="Expand target components")
    include_relations: bool = Field(default=True, description="Include target relations")
    normalize_sequences: bool = Field(default=True, description="Normalize protein sequences")


class TargetConfig(BaseConfig):
    """Configuration for target ETL pipeline."""

    # Target-specific sections
    sources: dict[str, TargetSourceSettings] = Field(default_factory=dict)
    io: TargetIOSettings = Field(default_factory=TargetIOSettings)
    runtime: TargetRuntimeSettings = Field(default_factory=TargetRuntimeSettings)

    @model_validator(mode="after")
    def _validate_target_config(self) -> TargetConfig:
        """Validate target-specific configuration."""
        # Ensure at least one source is enabled
        enabled_sources = [name for name, source in self.sources.items() if source.enabled]
        if not enabled_sources:
            raise ValueError("At least one data source must be enabled")

        return self


class TestitemSourceSettings(SourceConfigSection):
    """Configuration for testitem data sources."""

    # Testitem-specific settings
    include_inactive: bool = Field(default=False, description="Include inactive testitems")
    molecule_types: list[str] = Field(default_factory=list, description="Molecule types to include")
    include_synonyms: bool = Field(default=True, description="Include molecule synonyms")

    @field_validator("molecule_types")
    @classmethod
    def _validate_molecule_types(cls, v: list[str]) -> list[str]:
        """Validate molecule types."""
        valid_types = ["Small molecule", "Antibody", "Protein", "Nucleic acid", "Oligonucleotide"]
        for molecule_type in v:
            if molecule_type not in valid_types:
                raise ValueError(f"Invalid molecule type: {molecule_type}. Must be one of {valid_types}")
        return v


class TestitemIOSettings(IOSection):
    """I/O settings for testitem pipeline."""

    input_file: str = Field(default="testitems.csv", description="Input CSV file name")
    output_file: str = Field(default="testitems_normalized.parquet", description="Output file name")
    qc_report_file: str = Field(default="testitem_qc_report.json", description="QC report file name")


class TestitemRuntimeSettings(RuntimeSection):
    """Runtime settings for testitem pipeline."""

    # Testitem-specific runtime settings
    normalize_smiles: bool = Field(default=True, description="Normalize SMILES strings")
    include_properties: bool = Field(default=True, description="Include molecular properties")
    validate_structures: bool = Field(default=True, description="Validate molecular structures")


class TestitemConfig(BaseConfig):
    """Configuration for testitem ETL pipeline."""

    # Testitem-specific sections
    sources: dict[str, TestitemSourceSettings] = Field(default_factory=dict)
    io: TestitemIOSettings = Field(default_factory=TestitemIOSettings)
    runtime: TestitemRuntimeSettings = Field(default_factory=TestitemRuntimeSettings)

    @model_validator(mode="after")
    def _validate_testitem_config(self) -> TestitemConfig:
        """Validate testitem-specific configuration."""
        # Ensure at least one source is enabled
        enabled_sources = [name for name, source in self.sources.items() if source.enabled]
        if not enabled_sources:
            raise ValueError("At least one data source must be enabled")

        return self


# Configuration factory for creating entity-specific configurations
class EntityConfigFactory:
    """Factory for creating entity-specific configurations."""

    _config_classes = {
        "activity": ActivityConfig,
        "assay": AssayConfig,
        "document": DocumentConfig,
        "target": TargetConfig,
        "testitem": TestitemConfig,
    }

    @classmethod
    def create_config(cls, entity_type: str, data: dict[str, Any] | None = None) -> BaseConfig:
        """Create configuration for specific entity type."""
        if entity_type not in cls._config_classes:
            raise ValueError(f"Unknown entity type: {entity_type}. Must be one of {list(cls._config_classes.keys())}")

        config_class = cls._config_classes[entity_type]

        if data is None:
            return config_class()
        else:
            return config_class.model_validate(data)

    @classmethod
    def get_supported_entities(cls) -> list[str]:
        """Get list of supported entity types."""
        return list(cls._config_classes.keys())

    @classmethod
    def get_default_config(cls, entity_type: str) -> BaseConfig:
        """Get default configuration for entity type."""
        return cls.create_config(entity_type)
