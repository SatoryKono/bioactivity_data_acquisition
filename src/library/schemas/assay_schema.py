"""Pandera schemas for assay data validation."""

from __future__ import annotations

import importlib.util

from pandera.typing import Series

_PANDERA_PANDAS_SPEC = importlib.util.find_spec("pandera.pandas")
if _PANDERA_PANDAS_SPEC is not None:  # pragma: no cover - import side effect
    import pandera.pandas as pa  # type: ignore[no-redef]
else:  # pragma: no cover - import side effect
    import pandera as pa


class AssayInputSchema(pa.DataFrameModel):
    """Schema for input assay data from CSV files."""

    # Required fields
    assay_chembl_id: Series[str] = pa.Field(description="ChEMBL assay identifier")

    # Optional fields for target-based extraction
    target_chembl_id: Series[str] = pa.Field(nullable=True, description="ChEMBL target identifier")

    class Config:
        strict = True
        coerce = True


class AssayRawSchema(pa.DataFrameModel):
    """Schema for raw assay records fetched from the ChEMBL API.

    This schema validates raw data from ChEMBL API before normalization.
    """

    # Required fields - must be present in all records
    source_system: Series[str] = pa.Field(description="Data source identifier (e.g., 'ChEMBL')", nullable=False)
    extracted_at: Series[str] = pa.Field(description="Timestamp when data was retrieved from API", nullable=False)

    # Core assay fields - nullable=True because API may not always provide them
<<<<<<< Updated upstream
    assay_chembl_id: Series[str] = pa.Field(
        description="ChEMBL assay identifier",
        nullable=True
    )
    src_id: Series[object] = pa.Field(
        description="Source identifier",
        nullable=True
    )
    src_assay_id: Series[str] = pa.Field(
        description="Source assay identifier",
        nullable=True
    )
    
    # Classification fields
    assay_type: Series[str] = pa.Field(
        description="Assay type (B/F/P/U)",
        nullable=True
    )
    assay_type_description: Series[str] = pa.Field(
        description="Assay type description",
        nullable=True
    )
    bao_format: Series[str] = pa.Field(
        description="BAO format",
        nullable=True
    )
    bao_label: Series[str] = pa.Field(
        description="BAO label",
        nullable=True
    )
    
    # Target relationship fields
    target_chembl_id: Series[str] = pa.Field(
        description="ChEMBL target identifier",
        nullable=True
    )
    relationship_type: Series[str] = pa.Field(
        description="Relationship type to target",
        nullable=True
    )
    confidence_score: Series[object] = pa.Field(
        description="Confidence score (0-9)",
        nullable=True
    )
    
    # Biological context fields
    assay_organism: Series[str] = pa.Field(
        description="Assay organism",
        nullable=True
    )
    assay_tax_id: Series[object] = pa.Field(
        description="Organism taxonomic ID",
        nullable=True
    )
    assay_cell_type: Series[str] = pa.Field(
        description="Cell type",
        nullable=True
    )
    assay_tissue: Series[str] = pa.Field(
        description="Tissue type",
        nullable=True
    )
    assay_strain: Series[str] = pa.Field(
        description="Strain",
        nullable=True
    )
    assay_subcellular_fraction: Series[str] = pa.Field(
        description="Subcellular fraction",
        nullable=True
    )
    
    # Description and protocol fields
    description: Series[str] = pa.Field(
        description="Assay description",
        nullable=True
    )
    assay_format: Series[str] = pa.Field(
        description="Assay format",
        nullable=True
    )
    
    # Hash fields for deduplication
    hash_row: Series[str] = pa.Field(
        description="Row hash for deduplication",
        nullable=True
    )
    hash_business_key: Series[str] = pa.Field(
        description="Business key hash",
        nullable=True
    )
=======
    assay_chembl_id: Series[str] = pa.Field(description="ChEMBL assay identifier", nullable=True)
    assay_type: Series[str] = pa.Field(description="Assay type (B/F/P/U)", nullable=True)
    assay_category: Series[str] = pa.Field(description="Assay category", nullable=True)
    assay_cell_type: Series[str] = pa.Field(description="Cell type", nullable=True)
    assay_classifications: Series[str] = pa.Field(description="Assay classifications (JSON)", nullable=True)
    assay_group: Series[str] = pa.Field(description="Assay group", nullable=True)
    assay_organism: Series[str] = pa.Field(description="Assay organism", nullable=True)
    assay_parameters_json: Series[str] = pa.Field(description="Assay parameters as JSON string", nullable=True)
    assay_strain: Series[str] = pa.Field(description="Strain", nullable=True)
    assay_subcellular_fraction: Series[str] = pa.Field(description="Subcellular fraction", nullable=True)
    assay_tax_id: Series[object] = pa.Field(description="Organism taxonomic ID", nullable=True)
    assay_test_type: Series[str] = pa.Field(description="Assay test type", nullable=True)
    assay_tissue: Series[str] = pa.Field(description="Tissue type", nullable=True)
    assay_type_description: Series[str] = pa.Field(description="Assay type description", nullable=True)
    bao_format: Series[str] = pa.Field(description="BAO format", nullable=True)
    bao_label: Series[str] = pa.Field(description="BAO label", nullable=True)
    cell_chembl_id: Series[str] = pa.Field(description="ChEMBL cell identifier", nullable=True)
    confidence_description: Series[str] = pa.Field(description="Confidence description", nullable=True)
    confidence_score: Series[object] = pa.Field(description="Confidence score (0-9)", nullable=True)
    assay_description: Series[str] = pa.Field(description="Assay description", nullable=True)
    bao_endpoint: Series[str] = pa.Field(description="BAO endpoint", nullable=True)
    document_chembl_id: Series[str] = pa.Field(description="ChEMBL document identifier", nullable=True)
    relationship_description: Series[str] = pa.Field(description="Relationship description", nullable=True)
    relationship_type: Series[str] = pa.Field(description="Relationship type to target", nullable=True)
    src_assay_id: Series[str] = pa.Field(description="Source assay identifier", nullable=True)
    src_id: Series[object] = pa.Field(description="Source identifier", nullable=True)
    target_chembl_id: Series[str] = pa.Field(description="ChEMBL target identifier", nullable=True)
    tissue_chembl_id: Series[str] = pa.Field(description="ChEMBL tissue identifier", nullable=True)
    variant_sequence_json: Series[str] = pa.Field(description="Variant sequence as JSON string", nullable=True)

    # ASSAY_PARAMETERS (expanded from JSON)
    assay_param_type: Series[str] = pa.Field(description="Assay parameter type", nullable=True)
    assay_param_relation: Series[str] = pa.Field(description="Assay parameter relation", nullable=True)
    assay_param_value: Series[float] = pa.Field(description="Assay parameter value", nullable=True)
    assay_param_units: Series[str] = pa.Field(description="Assay parameter units", nullable=True)
    assay_param_text_value: Series[str] = pa.Field(description="Assay parameter text value", nullable=True)
    assay_param_standard_type: Series[str] = pa.Field(description="Assay parameter standard type", nullable=True)
    assay_param_standard_value: Series[float] = pa.Field(description="Assay parameter standard value", nullable=True)
    assay_param_standard_units: Series[str] = pa.Field(description="Assay parameter standard units", nullable=True)

    # ASSAY_CLASS (from /assay_class endpoint)
    assay_class_id: Series[int] = pa.Field(description="Assay class identifier", nullable=True)
    assay_class_bao_id: Series[str] = pa.Field(description="Assay class BAO ID", nullable=True)
    assay_class_type: Series[str] = pa.Field(description="Assay class type", nullable=True)
    assay_class_l1: Series[str] = pa.Field(description="Assay class hierarchy level 1", nullable=True)
    assay_class_l2: Series[str] = pa.Field(description="Assay class hierarchy level 2", nullable=True)
    assay_class_l3: Series[str] = pa.Field(description="Assay class hierarchy level 3", nullable=True)
    assay_class_description: Series[str] = pa.Field(description="Assay class description", nullable=True)

    # VARIANT_SEQUENCES (expanded from JSON)
    variant_id: Series[int] = pa.Field(description="Variant identifier", nullable=True)
    variant_base_accession: Series[str] = pa.Field(description="Variant base accession", nullable=True)
    variant_mutation: Series[str] = pa.Field(description="Variant mutation", nullable=True)
    variant_sequence: Series[str] = pa.Field(description="Variant sequence", nullable=True)
    variant_accession_reported: Series[str] = pa.Field(description="Variant accession reported", nullable=True)
>>>>>>> Stashed changes

    class Config:
        strict = True  # STRICT MODE: No additional columns allowed
        coerce = True  # Allow type coercion for data cleaning


class AssayNormalizedSchema(pa.DataFrameModel):
    """Schema for normalized assay data ready for export.

    This schema validates normalized data after ETL processing.
    All data should be clean and consistent for downstream analysis.
    """

    # Required fields - must be present in all normalized records
<<<<<<< Updated upstream
    source_system: Series[str] = pa.Field(
        description="Data source identifier (normalized)",
        nullable=False
    )
    extracted_at: Series[object] = pa.Field(
        description="Timestamp when data was retrieved from API",
        nullable=False
    )
    chembl_release: Series[str] = pa.Field(
        description="ChEMBL release version",
        nullable=False
    )
    
    # Core assay fields - nullable=True for missing data
    assay_chembl_id: Series[str] = pa.Field(
        description="ChEMBL assay identifier (business key)",
        nullable=False,
        unique=True
    )
    src_id: Series[object] = pa.Field(
        description="Source identifier",
        nullable=True
    )
    src_name: Series[str] = pa.Field(
        description="Source name",
        nullable=True
    )
    src_assay_id: Series[str] = pa.Field(
        description="Source assay identifier",
        nullable=True
    )
    
    # Classification fields
    assay_type: Series[str] = pa.Field(
        description="Assay type (B/F/P/U)",
        nullable=True
    )
    assay_type_description: Series[str] = pa.Field(
        description="Assay type description",
        nullable=True
    )
    bao_format: Series[str] = pa.Field(
        description="BAO format",
        nullable=True
    )
    bao_label: Series[str] = pa.Field(
        description="BAO label",
        nullable=True
    )
    assay_category: Series[object] = pa.Field(
        description="Assay category (list)",
        nullable=True
    )
    assay_classifications: Series[object] = pa.Field(
        description="Assay classifications (list)",
        nullable=True
    )
    
    # Target relationship fields
    target_chembl_id: Series[str] = pa.Field(
        description="ChEMBL target identifier",
        nullable=True
    )
    relationship_type: Series[str] = pa.Field(
        description="Relationship type to target",
        nullable=True
    )
    confidence_score: Series[object] = pa.Field(
        description="Confidence score (0-9)",
        nullable=True
    )
    
    # Biological context fields
    assay_organism: Series[str] = pa.Field(
        description="Assay organism",
        nullable=True
    )
    assay_tax_id: Series[object] = pa.Field(
        description="Organism taxonomic ID",
        nullable=True
    )
    assay_cell_type: Series[str] = pa.Field(
        description="Cell type",
        nullable=True
    )
    assay_tissue: Series[str] = pa.Field(
        description="Tissue type",
        nullable=True
    )
    assay_strain: Series[str] = pa.Field(
        description="Strain",
        nullable=True
    )
    assay_subcellular_fraction: Series[str] = pa.Field(
        description="Subcellular fraction",
        nullable=True
    )
    
    # Description and protocol fields
    description: Series[str] = pa.Field(
        description="Assay description",
        nullable=True
    )
    assay_parameters: Series[object] = pa.Field(
        description="Assay parameters (dict/str)",
        nullable=True
    )
    assay_format: Series[str] = pa.Field(
        description="Assay format",
        nullable=True
    )
    
    # Hash fields for deduplication
    hash_row: Series[str] = pa.Field(
        description="Row hash for deduplication",
        nullable=False
    )
    hash_business_key: Series[str] = pa.Field(
        description="Business key hash",
        nullable=False
    )
=======
    source_system: Series[str] = pa.Field(description="Data source identifier (normalized)", nullable=False)
    extracted_at: Series[object] = pa.Field(description="Timestamp when data was retrieved from API", nullable=False)
    chembl_release: Series[str] = pa.Field(description="ChEMBL release version (e.g., CHEMBL_36)", nullable=True, str_matches=r"^CHEMBL_\d+$")

    # Core assay fields - nullable=True for missing data
    assay_chembl_id: Series[str] = pa.Field(description="ChEMBL assay identifier (business key)", nullable=False, unique=True)
    assay_type: Series[str] = pa.Field(description="Assay type (B/F/P/U)", nullable=True)
    assay_category: Series[str] = pa.Field(description="Assay category", nullable=True)
    assay_cell_type: Series[str] = pa.Field(description="Cell type", nullable=True)
    assay_classifications: Series[str] = pa.Field(description="Assay classifications (JSON)", nullable=True)
    assay_group: Series[str] = pa.Field(description="Assay group", nullable=True)
    assay_organism: Series[str] = pa.Field(description="Assay organism", nullable=True)
    assay_parameters_json: Series[str] = pa.Field(description="Assay parameters as JSON string", nullable=True)
    assay_strain: Series[str] = pa.Field(description="Strain", nullable=True)
    assay_subcellular_fraction: Series[str] = pa.Field(description="Subcellular fraction", nullable=True)
    assay_tax_id: Series[object] = pa.Field(description="Organism taxonomic ID", nullable=True)
    assay_test_type: Series[str] = pa.Field(description="Assay test type", nullable=True)
    assay_tissue: Series[str] = pa.Field(description="Tissue type", nullable=True)
    assay_type_description: Series[str] = pa.Field(description="Assay type description", nullable=True)
    bao_format: Series[str] = pa.Field(description="BAO format", nullable=True)
    bao_label: Series[str] = pa.Field(description="BAO label", nullable=True)
    cell_chembl_id: Series[str] = pa.Field(description="ChEMBL cell identifier", nullable=True)
    confidence_description: Series[str] = pa.Field(description="Confidence description", nullable=True)
    confidence_score: Series[object] = pa.Field(description="Confidence score (0-9)", nullable=True)
    assay_description: Series[str] = pa.Field(description="Assay description", nullable=True)
    bao_endpoint: Series[str] = pa.Field(description="BAO endpoint", nullable=True)
    document_chembl_id: Series[str] = pa.Field(description="ChEMBL document identifier", nullable=True)
    relationship_description: Series[str] = pa.Field(description="Relationship description", nullable=True)
    relationship_type: Series[str] = pa.Field(description="Relationship type to target", nullable=True)
    src_assay_id: Series[str] = pa.Field(description="Source assay identifier", nullable=True)
    src_id: Series[object] = pa.Field(description="Source identifier", nullable=True)
    target_chembl_id: Series[str] = pa.Field(description="ChEMBL target identifier", nullable=True)
    tissue_chembl_id: Series[str] = pa.Field(description="ChEMBL tissue identifier", nullable=True)
    variant_sequence_json: Series[str] = pa.Field(description="Variant sequence as JSON string", nullable=True)

    # ASSAY_PARAMETERS (expanded from JSON)
    assay_param_type: Series[str] = pa.Field(description="Assay parameter type", nullable=True)
    assay_param_relation: Series[str] = pa.Field(description="Assay parameter relation", nullable=True, isin=["=", ">", ">=", "<", "<=", "~"])
    assay_param_value: Series[float] = pa.Field(description="Assay parameter value", nullable=True)
    assay_param_units: Series[str] = pa.Field(description="Assay parameter units", nullable=True)
    assay_param_text_value: Series[str] = pa.Field(description="Assay parameter text value", nullable=True)
    assay_param_standard_type: Series[str] = pa.Field(description="Assay parameter standard type", nullable=True)
    assay_param_standard_value: Series[float] = pa.Field(description="Assay parameter standard value", nullable=True)
    assay_param_standard_units: Series[str] = pa.Field(description="Assay parameter standard units", nullable=True)

    # ASSAY_CLASS (from /assay_class endpoint)
    assay_class_id: Series[int] = pa.Field(description="Assay class identifier", nullable=True)
    assay_class_bao_id: Series[str] = pa.Field(description="Assay class BAO ID", nullable=True, str_matches=r"^BAO_\d{7}$")
    assay_class_type: Series[str] = pa.Field(description="Assay class type", nullable=True)
    assay_class_l1: Series[str] = pa.Field(description="Assay class hierarchy level 1", nullable=True)
    assay_class_l2: Series[str] = pa.Field(description="Assay class hierarchy level 2", nullable=True)
    assay_class_l3: Series[str] = pa.Field(description="Assay class hierarchy level 3", nullable=True)
    assay_class_description: Series[str] = pa.Field(description="Assay class description", nullable=True)

    # VARIANT_SEQUENCES (expanded from JSON)
    variant_id: Series[int] = pa.Field(description="Variant identifier", nullable=True)
    variant_base_accession: Series[str] = pa.Field(description="Variant base accession", nullable=True, str_matches=r"^[OPQ][0-9][A-Z0-9]{3}[0-9](-\d+)?$")
    variant_mutation: Series[str] = pa.Field(description="Variant mutation", nullable=True)
    variant_sequence: Series[str] = pa.Field(description="Variant sequence", nullable=True, str_matches=r"^[A-Z\*]+$")
    variant_accession_reported: Series[str] = pa.Field(description="Variant accession reported", nullable=True)

    # Hash fields for deduplication
    hash_row: Series[str] = pa.Field(description="Row hash for deduplication", nullable=False)
    hash_business_key: Series[str] = pa.Field(description="Business key hash", nullable=False)

    # System fields
    index: Series[int] = pa.Field(description="Row index for deterministic ordering", nullable=False)
    pipeline_version: Series[str] = pa.Field(description="ETL pipeline version", nullable=False)
>>>>>>> Stashed changes

    class Config:
        strict = True  # STRICT MODE: No additional columns allowed
        coerce = True  # Allow type coercion for data cleaning


__all__ = ["AssayInputSchema", "AssayRawSchema", "AssayNormalizedSchema"]
