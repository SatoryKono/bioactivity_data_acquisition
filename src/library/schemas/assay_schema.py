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
    source_system: Series[str] = pa.Field(
        description="Data source identifier (e.g., 'ChEMBL')",
        nullable=False
    )
    extracted_at: Series[str] = pa.Field(
        description="Timestamp when data was retrieved from API",
        nullable=False
    )
    
    # Core assay fields - nullable=True because API may not always provide them
    assay_chembl_id: Series[str] = pa.Field(
        description="ChEMBL assay identifier",
        nullable=True
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
    
    # Variant fields from variant_sequence (extracted from /assay endpoint)
    variant_id: Series[object] = pa.Field(
        description="ChEMBL variant identifier",
        nullable=True
    )
    variant_text: Series[str] = pa.Field(
        description="Variant description text",
        nullable=True
    )
    variant_sequence_id: Series[object] = pa.Field(
        description="Variant sequence identifier",
        nullable=True
    )
    isoform: Series[object] = pa.Field(
        description="Isoform from variant_sequence",
        nullable=True
    )
    mutation: Series[str] = pa.Field(
        description="Mutation from variant_sequence",
        nullable=True
    )
    sequence: Series[str] = pa.Field(
        description="Sequence from variant_sequence",
        nullable=True
    )
    variant_accession: Series[str] = pa.Field(
        description="Accession from variant_sequence",
        nullable=True
    )
    variant_organism: Series[str] = pa.Field(
        description="Organism from variant_sequence",
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
    assay_parameters_json: Series[str] = pa.Field(
        description="Assay parameters as normalized deterministic JSON string",
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

    class Config:
        strict = True  # STRICT MODE: No additional columns allowed
        coerce = True  # Allow type coercion for data cleaning


class AssayNormalizedSchema(pa.DataFrameModel):
    """Schema for normalized assay data ready for export.
    
    This schema validates normalized data after ETL processing.
    All data should be clean and consistent for downstream analysis.
    """

    # Required fields - must be present in all normalized records
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
    
    # Variant fields (normalized from variant_sequence)
    variant_id: Series[object] = pa.Field(
        description="ChEMBL variant identifier",
        nullable=True
    )
    is_variant: Series[bool] = pa.Field(
        description="Flag indicating if assay has variant data",
        nullable=False
    )
    variant_accession: Series[str] = pa.Field(
        description="UniProt accession of variant",
        nullable=True
    )
    variant_mutations: Series[str] = pa.Field(
        description="Raw description of variant mutations",
        nullable=True
    )
    variant_sequence: Series[str] = pa.Field(
        description="Variant protein sequence",
        nullable=True
    )
    variant_text: Series[str] = pa.Field(
        description="Variant description text from assay",
        nullable=True
    )
    variant_sequence_id: Series[object] = pa.Field(
        description="Variant sequence identifier",
        nullable=True
    )
    target_uniprot_accession: Series[str] = pa.Field(
        description="Base UniProt accession of target",
        nullable=True
    )
    target_isoform: Series[object] = pa.Field(
        description="Target isoform number (from variant_sequence.isoform)",
        nullable=True
    )
    variant_organism: Series[str] = pa.Field(
        description="Organism from variant_sequence",
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
        description="Assay parameters (dict/str) - legacy field",
        nullable=True
    )
    assay_parameters_json: Series[str] = pa.Field(
        description="Assay parameters as normalized deterministic JSON string",
        nullable=False
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

    class Config:
        strict = True  # STRICT MODE: No additional columns allowed
        coerce = True  # Allow type coercion for data cleaning


__all__ = ["AssayInputSchema", "AssayRawSchema", "AssayNormalizedSchema"]
