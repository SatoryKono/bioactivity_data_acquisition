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

    # Pipeline information
    pipeline_name: Series[str] = pa.Field(description="Pipeline name")
    pipeline_version: Series[str] = pa.Field(description="Pipeline version")
    entity_type: Series[str] = pa.Field(description="Entity type (documents, targets, etc.)")
    source_system: Series[str] = pa.Field(description="Source system (chembl, etc.)")

    # Execution information
    run_id: Series[str] = pa.Field(description="Unique run identifier (UUID)")
    started_at: Series[str] = pa.Field(description="ISO timestamp when execution started")
    completed_at: Series[str] = pa.Field(description="ISO timestamp when execution completed")
    duration_sec: Series[float] = pa.Field(description="Execution duration in seconds")

    # Data statistics
    row_count: Series[int] = pa.Field(description="Total number of rows")
    row_count_accepted: Series[int] = pa.Field(description="Number of accepted rows")
    row_count_rejected: Series[int] = pa.Field(description="Number of rejected rows")
    columns_count: Series[int] = pa.Field(description="Number of columns")

    # Validation results
    schema_passed: Series[bool] = pa.Field(description="Whether schema validation passed")
    qc_passed: Series[bool] = pa.Field(description="Whether QC validation passed")
    warnings: Series[int] = pa.Field(description="Number of warnings")
    errors: Series[int] = pa.Field(description="Number of errors")

    class Config:
        """Pandera configuration."""

        strict = False  # Allow additional fields
        coerce = True  # Coerce types when possible


def validate_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    """Validate metadata dictionary against the schema.

    Args:
        metadata: Dictionary containing metadata to validate

    Returns:
        Validated metadata dictionary

    Raises:
        pa.errors.SchemaError: If validation fails
    """
    # Flatten nested structure for validation
    flattened = {}

    # Extract pipeline info
    if "pipeline" in metadata:
        pipeline = metadata["pipeline"]
        flattened["pipeline_name"] = pipeline.get("name")
        flattened["pipeline_version"] = pipeline.get("version")
        flattened["entity_type"] = pipeline.get("entity_type")
        flattened["source_system"] = pipeline.get("source_system")

    # Extract execution info
    if "execution" in metadata:
        execution = metadata["execution"]
        flattened["run_id"] = execution.get("run_id")
        flattened["started_at"] = execution.get("started_at")
        flattened["completed_at"] = execution.get("completed_at")
        flattened["duration_sec"] = execution.get("duration_sec")

    # Extract data info
    if "data" in metadata:
        data = metadata["data"]
        flattened["row_count"] = data.get("row_count")
        flattened["row_count_accepted"] = data.get("row_count_accepted")
        flattened["row_count_rejected"] = data.get("row_count_rejected")
        flattened["columns_count"] = data.get("columns_count")

    # Extract validation info
    if "validation" in metadata:
        validation = metadata["validation"]
        flattened["schema_passed"] = validation.get("schema_passed")
        flattened["qc_passed"] = validation.get("qc_passed")
        flattened["warnings"] = validation.get("warnings")
        flattened["errors"] = validation.get("errors")

    # Convert to DataFrame for validation
    import pandas as pd

    df = pd.DataFrame([flattened])

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


__all__ = ["DatasetMetadataSchema", "validate_metadata", "validate_metadata_file"]
