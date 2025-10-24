"""Tests for metadata schema validation."""

import tempfile
from pathlib import Path

import pandas as pd
import pandera as pa
import pytest
import yaml

from library.schemas.meta_schema import (
    DatasetMetadataSchema,
    validate_metadata,
    validate_metadata_file,
)


class TestDatasetMetadataSchema:
    """Test cases for DatasetMetadataSchema."""
    
    def test_valid_metadata(self):
        """Test validation of valid metadata."""
        valid_data = {
            "dataset": "chembl",
            "run_id": "123e4567-e89b-12d3-a456-426614174000",
            "generated_at": "2024-01-15T10:00:00Z",
            "chembl_db_version": "ChEMBL_33",
            "chembl_release_date": "2024-01-15",
            "chembl_status": "ok",
            "pipeline_version": "1.0.0",
            "row_count": 1000
        }
        
        df = pd.DataFrame([valid_data])
        validated_df = DatasetMetadataSchema.validate(df)
        
        assert len(validated_df) == 1
        assert validated_df.iloc[0]["dataset"] == "chembl"
        assert validated_df.iloc[0]["chembl_db_version"] == "ChEMBL_33"
    
    def test_metadata_with_nulls(self):
        """Test validation of metadata with null values."""
        data_with_nulls = {
            "dataset": "chembl",
            "run_id": "123e4567-e89b-12d3-a456-426614174000",
            "generated_at": "2024-01-15T10:00:00Z",
            "chembl_db_version": None,
            "chembl_release_date": None,
            "chembl_status": None,
            "pipeline_version": "1.0.0",
            "row_count": 0
        }
        
        df = pd.DataFrame([data_with_nulls])
        validated_df = DatasetMetadataSchema.validate(df)
        
        assert len(validated_df) == 1
        assert validated_df.iloc[0]["chembl_db_version"] is None
        assert validated_df.iloc[0]["chembl_release_date"] is None
    
    def test_metadata_missing_required_fields(self):
        """Test validation fails with missing required fields."""
        incomplete_data = {
            "dataset": "chembl",
            # Missing run_id and generated_at
            "chembl_db_version": "ChEMBL_33"
        }
        
        df = pd.DataFrame([incomplete_data])
        
        with pytest.raises(pa.errors.SchemaError):
            DatasetMetadataSchema.validate(df)
    
    def test_metadata_extra_fields(self):
        """Test validation allows extra fields."""
        data_with_extra = {
            "dataset": "chembl",
            "run_id": "123e4567-e89b-12d3-a456-426614174000",
            "generated_at": "2024-01-15T10:00:00Z",
            "chembl_db_version": "ChEMBL_33",
            "extra_field": "extra_value",
            "another_field": 123
        }
        
        df = pd.DataFrame([data_with_extra])
        validated_df = DatasetMetadataSchema.validate(df)
        
        assert len(validated_df) == 1
        assert validated_df.iloc[0]["dataset"] == "chembl"
        # Extra fields should be preserved
        assert validated_df.iloc[0]["extra_field"] == "extra_value"
        assert validated_df.iloc[0]["another_field"] == 123


class TestValidateMetadata:
    """Test cases for validate_metadata function."""
    
    def test_validate_metadata_success(self):
        """Test successful metadata validation."""
        valid_metadata = {
            "dataset": "chembl",
            "run_id": "123e4567-e89b-12d3-a456-426614174000",
            "generated_at": "2024-01-15T10:00:00Z",
            "chembl_db_version": "ChEMBL_33",
            "chembl_release_date": "2024-01-15"
        }
        
        result = validate_metadata(valid_metadata)
        
        assert result["dataset"] == "chembl"
        assert result["chembl_db_version"] == "ChEMBL_33"
        assert result["chembl_release_date"] == "2024-01-15"
    
    def test_validate_metadata_failure(self):
        """Test metadata validation failure."""
        invalid_metadata = {
            "dataset": "chembl",
            # Missing required fields
            "chembl_db_version": "ChEMBL_33"
        }
        
        with pytest.raises(pa.errors.SchemaError):
            validate_metadata(invalid_metadata)


class TestValidateMetadataFile:
    """Test cases for validate_metadata_file function."""
    
    def test_validate_metadata_file_success(self):
        """Test successful metadata file validation."""
        valid_metadata = {
            "dataset": "chembl",
            "run_id": "123e4567-e89b-12d3-a456-426614174000",
            "generated_at": "2024-01-15T10:00:00Z",
            "chembl_db_version": "ChEMBL_33",
            "chembl_release_date": "2024-01-15"
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(valid_metadata, f)
            temp_path = f.name
        
        try:
            result = validate_metadata_file(temp_path)
            
            assert result["dataset"] == "chembl"
            assert result["chembl_db_version"] == "ChEMBL_33"
            assert result["chembl_release_date"] == "2024-01-15"
        finally:
            Path(temp_path).unlink()
    
    def test_validate_metadata_file_not_found(self):
        """Test metadata file validation with non-existent file."""
        with pytest.raises(FileNotFoundError):
            validate_metadata_file("non_existent_file.yaml")
    
    def test_validate_metadata_file_invalid_yaml(self):
        """Test metadata file validation with invalid YAML."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [")
            temp_path = f.name
        
        try:
            with pytest.raises(yaml.YAMLError):
                validate_metadata_file(temp_path)
        finally:
            Path(temp_path).unlink()
    
    def test_validate_metadata_file_invalid_schema(self):
        """Test metadata file validation with invalid schema."""
        invalid_metadata = {
            "dataset": "chembl",
            # Missing required fields
            "chembl_db_version": "ChEMBL_33"
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(invalid_metadata, f)
            temp_path = f.name
        
        try:
            with pytest.raises(pa.errors.SchemaError):
                validate_metadata_file(temp_path)
        finally:
            Path(temp_path).unlink()
