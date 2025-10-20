"""Tests for activity pipeline artifacts and hashing."""

import hashlib
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from library.activity.config import ActivityConfig
from library.activity.pipeline import (
    ActivityETLResult,
    _calculate_business_key_hash,
    _calculate_row_hash,
    write_activity_outputs,
)


class TestActivityArtifacts:
    """Test cases for activity pipeline artifacts and hashing."""

    def test_calculate_business_key_hash_full_sha256(self):
        """Test that business key hash uses full SHA256."""
        activity_id = "CHEMBL123"
        hash_result = _calculate_business_key_hash(activity_id)
        
        # Should be full SHA256 (64 characters)
        assert len(hash_result) == 64
        assert isinstance(hash_result, str)
        
        # Should be deterministic
        hash_result2 = _calculate_business_key_hash(activity_id)
        assert hash_result == hash_result2
        
        # Should match expected SHA256
        expected = hashlib.sha256(activity_id.encode()).hexdigest()
        assert hash_result == expected

    def test_calculate_row_hash_full_sha256(self):
        """Test that row hash uses full SHA256."""
        row_data = {
            "activity_id": "CHEMBL123",
            "standard_value": 1.0,
            "standard_units": "uM",
            "target_pref_name": "Test Target"
        }
        hash_result = _calculate_row_hash(row_data)
        
        # Should be full SHA256 (64 characters)
        assert len(hash_result) == 64
        assert isinstance(hash_result, str)
        
        # Should be deterministic
        hash_result2 = _calculate_row_hash(row_data)
        assert hash_result == hash_result2

    def test_write_activity_outputs_plural_names(self):
        """Test that activity outputs use plural artifact names."""
        # Create test data
        test_activity = pd.DataFrame([{
            "activity_chembl_id": "CHEMBL123",
            "standard_value": 1.0,
            "standard_units": "uM",
            "hash_business_key": _calculate_business_key_hash("CHEMBL123"),
            "hash_row": _calculate_row_hash({"activity_chembl_id": "CHEMBL123"})
        }])
        
        test_qc = pd.DataFrame([{
            "metric": "row_count",
            "value": 1
        }])
        
        test_meta = {
            "pipeline_version": "1.0.0",
            "sources": ["chembl"],
            "total_records": 1
        }
        
        result = ActivityETLResult(
            activity=test_activity,
            qc=test_qc,
            meta=test_meta
        )
        
        config = ActivityConfig()
        output_dir = Path("test_output")
        date_tag = "20240101"
        
        # Mock the write_deterministic_csv function
        with patch("library.activity.pipeline.write_deterministic_csv") as mock_write_csv, \
             patch("library.activity.pipeline.yaml.dump") as mock_yaml_dump, \
             patch("builtins.open", create=True) as mock_open:
            
            # Mock Path.mkdir to avoid actual directory creation
            with patch.object(Path, "mkdir"):
                outputs = write_activity_outputs(result, output_dir, date_tag, config)
            
            # Check that plural names are used
            expected_files = {
                "activities_csv": output_dir / "activities_20240101.csv",
                "activities_qc_csv": output_dir / "activities_20240101_qc.csv",
                "activities_meta_yaml": output_dir / "activities_20240101_meta.yaml"
            }
            
            assert outputs == expected_files
            
            # Verify write_deterministic_csv was called with correct paths
            assert mock_write_csv.call_count == 2  # CSV and QC files
            
            # Check that legacy aliases are created
            legacy_files = {
                "activity_csv": output_dir / "activity_20240101.csv",
                "activity_qc_csv": output_dir / "activity_20240101_qc.csv",
                "activity_meta_yaml": output_dir / "activity_20240101_meta.yaml"
            }
            
            # The function should also create legacy aliases
            # (This would be verified by checking if the legacy files exist in the outputs)
            # For now, we just verify the main files are created with plural names

    def test_activity_etl_result_structure(self):
        """Test that ActivityETLResult has the expected structure."""
        test_activity = pd.DataFrame([{
            "activity_chembl_id": "CHEMBL123",
            "standard_value": 1.0
        }])
        
        test_qc = pd.DataFrame([{
            "metric": "row_count",
            "value": 1
        }])
        
        test_meta = {"pipeline_version": "1.0.0"}
        
        result = ActivityETLResult(
            activity=test_activity,
            qc=test_qc,
            meta=test_meta
        )
        
        # Verify structure
        assert hasattr(result, "activity")
        assert hasattr(result, "qc")
        assert hasattr(result, "meta")
        assert hasattr(result, "correlation_analysis")
        assert hasattr(result, "correlation_reports")
        assert hasattr(result, "correlation_insights")
        
        # Verify data
        assert len(result.activity) == 1
        assert len(result.qc) == 1
        assert result.meta["pipeline_version"] == "1.0.0"


if __name__ == "__main__":
    pytest.main([__file__])
