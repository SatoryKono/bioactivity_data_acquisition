"""Tests for strict Pandera schemas validation."""

import pandas as pd
import pytest
from pandera.errors import SchemaError

from library.schemas import RawBioactivitySchema, NormalizedBioactivitySchema


class TestRawBioactivitySchema:
    """Test strict validation for raw bioactivity data."""

    def test_valid_raw_data_passes(self):
        """Valid raw data should pass strict validation."""
        data = pd.DataFrame({
            "source": ["chembl", "crossref"],
            "retrieved_at": [pd.Timestamp.now(), pd.Timestamp.now()],
            "target_pref_name": ["EGFR", "VEGFR2"],
            "standard_value": [1.5, 2.3],
            "standard_units": ["nM", "uM"],
            "canonical_smiles": ["CCO", "CCN"],
            "activity_id": [12345, 67890],
            "assay_chembl_id": ["CHEMBL123", "CHEMBL456"],
            "document_chembl_id": ["CHEMBL789", "CHEMBL012"],
            "standard_type": ["IC50", "Ki"],
            "standard_relation": ["=", "<"],
            "target_chembl_id": ["CHEMBL345", "CHEMBL678"],
            "target_organism": ["Homo sapiens", "Mus musculus"],
            "target_tax_id": ["9606", "10090"]
        })
        
        # Should not raise any exception
        validated = RawBioactivitySchema.validate(data)
        assert len(validated) == 2

    def test_missing_required_fields_fails(self):
        """Missing required fields should fail validation."""
        data = pd.DataFrame({
            "source": ["chembl"],
            # Missing retrieved_at
            "target_pref_name": ["EGFR"]
        })
        
        with pytest.raises(SchemaError):
            RawBioactivitySchema.validate(data)

    def test_extra_columns_fail_in_strict_mode(self):
        """Extra columns should fail in strict mode."""
        data = pd.DataFrame({
            "source": ["chembl"],
            "retrieved_at": [pd.Timestamp.now()],
            "target_pref_name": ["EGFR"],
            "extra_column": ["should_fail"]  # This should cause failure
        })
        
        with pytest.raises(SchemaError):
            RawBioactivitySchema.validate(data)

    def test_empty_strings_fail_validation(self):
        """Empty strings should fail validation for non-nullable fields."""
        data = pd.DataFrame({
            "source": [""],  # Empty string should fail
            "retrieved_at": [pd.Timestamp.now()]
        })
        
        with pytest.raises(SchemaError):
            RawBioactivitySchema.validate(data)


class TestNormalizedBioactivitySchema:
    """Test strict validation for normalized bioactivity data."""

    def test_valid_normalized_data_passes(self):
        """Valid normalized data should pass strict validation."""
        data = pd.DataFrame({
            "source": ["chembl", "crossref"],
            "retrieved_at": [pd.Timestamp.now(), pd.Timestamp.now()],
            "target": ["EGFR", "VEGFR2"],
            "activity_value": [1.5, 2.3],
            "activity_unit": ["nM", "nM"],
            "smiles": ["CCO", "CCN"]
        })
        
        # Should not raise any exception
        validated = NormalizedBioactivitySchema.validate(data)
        assert len(validated) == 2

    def test_missing_required_fields_fails(self):
        """Missing required fields should fail validation."""
        data = pd.DataFrame({
            "source": ["chembl"],
            # Missing retrieved_at
            "target": ["EGFR"]
        })
        
        with pytest.raises(SchemaError):
            NormalizedBioactivitySchema.validate(data)

    def test_extra_columns_fail_in_strict_mode(self):
        """Extra columns should fail in strict mode."""
        data = pd.DataFrame({
            "source": ["chembl"],
            "retrieved_at": [pd.Timestamp.now()],
            "target": ["EGFR"],
            "extra_column": ["should_fail"]  # This should cause failure
        })
        
        with pytest.raises(SchemaError):
            NormalizedBioactivitySchema.validate(data)

    def test_empty_strings_fail_validation(self):
        """Empty strings should fail validation for non-nullable fields."""
        data = pd.DataFrame({
            "source": [""],  # Empty string should fail
            "retrieved_at": [pd.Timestamp.now()]
        })
        
        with pytest.raises(SchemaError):
            NormalizedBioactivitySchema.validate(data)

    def test_nullable_fields_allow_none(self):
        """Nullable fields should allow None values."""
        data = pd.DataFrame({
            "source": ["chembl"],
            "retrieved_at": [pd.Timestamp.now()],
            "target": [None],  # This should be allowed
            "activity_value": [None],  # This should be allowed
            "activity_unit": [None],  # This should be allowed
            "smiles": [None]  # This should be allowed
        })
        
        # Should not raise any exception
        validated = NormalizedBioactivitySchema.validate(data)
        assert len(validated) == 1
        assert validated["target"].iloc[0] is None


class TestSchemaCompatibility:
    """Test compatibility between raw and normalized schemas."""

    def test_schema_field_names_compatibility(self):
        """Test that field names are compatible between raw and normalized schemas."""
        # Test that the core fields exist in both schemas
        raw_schema = RawBioactivitySchema.to_schema()
        normalized_schema = NormalizedBioactivitySchema.to_schema()
        
        # Check that required fields exist in both schemas
        assert "source" in raw_schema.columns
        assert "source" in normalized_schema.columns
        assert "retrieved_at" in raw_schema.columns
        assert "retrieved_at" in normalized_schema.columns
        
        # Check that mapping fields exist
        assert "target_pref_name" in raw_schema.columns
        assert "target" in normalized_schema.columns
        assert "standard_value" in raw_schema.columns
        assert "activity_value" in normalized_schema.columns
        assert "standard_units" in raw_schema.columns
        assert "activity_unit" in normalized_schema.columns
        assert "canonical_smiles" in raw_schema.columns
        assert "smiles" in normalized_schema.columns
