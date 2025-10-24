"""Schema validation for dataset metadata (meta.yaml files)."""

from __future__ import annotations

from typing import Any

import pandera.pandas as pa
from pandera.typing import Series


class DatasetMetadataSchema(pa.DataFrameModel):
    """Schema for validating dataset metadata structure.
    
    This schema validates the structure of meta.yaml files created by ETL pipelines.
    It ensures that required fields are present and have correct types.
    """
    
    # Required fields for meta.yaml
    dataset: Series[str] = pa.Field(description="Dataset name (e.g., 'chembl')")
    run_id: Series[str] = pa.Field(description="Unique run identifier (UUID)")
    generated_at: Series[str] = pa.Field(description="ISO timestamp when metadata was generated")
    
    # ChEMBL version information (required for ChEMBL datasets)
    chembl_db_version: Series[str] = pa.Field(
        nullable=True,
        description="ChEMBL database version (e.g., 'ChEMBL_33')"
    )
    chembl_release_date: Series[str] = pa.Field(
        nullable=True,
        description="ChEMBL release date in ISO format (YYYY-MM-DD)"
    )
    
    # Optional fields
    chembl_status: Series[str] = pa.Field(
        nullable=True,
        description="ChEMBL API status"
    )
    chembl_status_timestamp: Series[str] = pa.Field(
        nullable=True,
        description="Timestamp when ChEMBL status was retrieved"
    )
    
    # Pipeline-specific fields
    pipeline_version: Series[str] = pa.Field(
        nullable=True,
        description="Version of the ETL pipeline"
    )
    row_count: Series[int] = pa.Field(
        nullable=True,
        description="Number of rows in the dataset"
    )
    
    class Config:
        """Pandera configuration."""
        strict = False  # Allow additional fields
        coerce = True   # Coerce types when possible


def validate_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    """Validate metadata dictionary against the schema.
    
    Args:
        metadata: Dictionary containing metadata to validate
        
    Returns:
        Validated metadata dictionary
        
    Raises:
        pa.errors.SchemaError: If validation fails
    """
    # Convert single metadata dict to DataFrame for validation
    import pandas as pd
    
    # Create a single-row DataFrame from the metadata
    df = pd.DataFrame([metadata])
    
    # Validate using the schema
    validated_df = DatasetMetadataSchema.validate(df)
    
    # Return the validated metadata as a dictionary
    return validated_df.iloc[0].to_dict()


def validate_metadata_file(file_path: str | pa.typing.Path) -> dict[str, Any]:
    """Validate metadata from a YAML file.
    
    Args:
        file_path: Path to the metadata YAML file
        
    Returns:
        Validated metadata dictionary
        
    Raises:
        pa.errors.SchemaError: If validation fails
        FileNotFoundError: If the file doesn't exist
        yaml.YAMLError: If the YAML file is malformed
    """
    from pathlib import Path
    
    import yaml
    
    path_obj = Path(file_path)
    if not path_obj.exists():
        raise FileNotFoundError(f"Metadata file not found: {path_obj}")
    
    # Load YAML file
    with path_obj.open("r", encoding="utf-8") as f:
        metadata = yaml.safe_load(f)
    
    # Validate the metadata
    return validate_metadata(metadata)


__all__ = [
    "DatasetMetadataSchema",
    "validate_metadata",
    "validate_metadata_file"
]
