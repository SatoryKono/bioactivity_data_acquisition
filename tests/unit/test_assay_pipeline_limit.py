#!/usr/bin/env python3
"""Unit tests for assay pipeline limit functionality."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch

from library.assay import AssayConfig, AssayPipeline


class TestAssayPipelineLimit:
    """Test cases for assay pipeline limit functionality."""
    
    def test_limit_applied_correctly(self):
        """Test that limit is applied correctly in pipeline.extract()."""
        # Create test data with 10 assay IDs
        test_data = pd.DataFrame({
            'assay_chembl_id': [f'CHEMBL{i:07d}' for i in range(100, 110)]
        })
        
        # Create config with limit of 5
        config = AssayConfig()
        config.runtime.limit = 5
        
        # Create pipeline
        pipeline = AssayPipeline(config)
        
        # Mock the ChEMBL client to avoid API calls
        with patch.object(pipeline, '_setup_clients'), \
             patch.object(pipeline, '_extract_from_chembl') as mock_extract, \
             patch.object(pipeline, '_merge_chembl_data') as mock_merge:
            
            # Mock the extract method to return test data
            mock_extract.return_value = test_data
            mock_merge.return_value = test_data
            
            # Run extract method
            result = pipeline.extract(test_data)
            
            # Verify that only 5 records are returned
            assert len(result) == 5
            assert result['assay_chembl_id'].tolist() == [f'CHEMBL{i:07d}' for i in range(100, 105)]
    
    def test_no_limit_when_none(self):
        """Test that no limit is applied when config.runtime.limit is None."""
        # Create test data with 10 assay IDs
        test_data = pd.DataFrame({
            'assay_chembl_id': [f'CHEMBL{i:07d}' for i in range(100, 110)]
        })
        
        # Create config with no limit
        config = AssayConfig()
        config.runtime.limit = None
        
        # Create pipeline
        pipeline = AssayPipeline(config)
        
        # Mock the ChEMBL client to avoid API calls
        with patch.object(pipeline, '_setup_clients'), \
             patch.object(pipeline, '_extract_from_chembl') as mock_extract, \
             patch.object(pipeline, '_merge_chembl_data') as mock_merge:
            
            # Mock the extract method to return test data
            mock_extract.return_value = test_data
            mock_merge.return_value = test_data
            
            # Run extract method
            result = pipeline.extract(test_data)
            
            # Verify that all 10 records are returned
            assert len(result) == 10
            assert result['assay_chembl_id'].tolist() == [f'CHEMBL{i:07d}' for i in range(100, 110)]
    
    def test_limit_zero_returns_empty(self):
        """Test that limit of 0 returns empty DataFrame."""
        # Create test data with 10 assay IDs
        test_data = pd.DataFrame({
            'assay_chembl_id': [f'CHEMBL{i:07d}' for i in range(100, 110)]
        })
        
        # Create config with limit of 0
        config = AssayConfig()
        config.runtime.limit = 0
        
        # Create pipeline
        pipeline = AssayPipeline(config)
        
        # Mock the ChEMBL client to avoid API calls
        with patch.object(pipeline, '_setup_clients'), \
             patch.object(pipeline, '_extract_from_chembl') as mock_extract, \
             patch.object(pipeline, '_merge_chembl_data') as mock_merge:
            
            # Mock the extract method to return test data
            mock_extract.return_value = test_data
            mock_merge.return_value = test_data
            
            # Run extract method
            result = pipeline.extract(test_data)
            
            # Verify that no records are returned
            assert len(result) == 0
            assert result.empty
    
    def test_limit_larger_than_data(self):
        """Test that limit larger than data size returns all data."""
        # Create test data with 5 assay IDs
        test_data = pd.DataFrame({
            'assay_chembl_id': [f'CHEMBL{i:07d}' for i in range(100, 105)]
        })
        
        # Create config with limit of 10 (larger than data)
        config = AssayConfig()
        config.runtime.limit = 10
        
        # Create pipeline
        pipeline = AssayPipeline(config)
        
        # Mock the ChEMBL client to avoid API calls
        with patch.object(pipeline, '_setup_clients'), \
             patch.object(pipeline, '_extract_from_chembl') as mock_extract, \
             patch.object(pipeline, '_merge_chembl_data') as mock_merge:
            
            # Mock the extract method to return test data
            mock_extract.return_value = test_data
            mock_merge.return_value = test_data
            
            # Run extract method
            result = pipeline.extract(test_data)
            
            # Verify that all 5 records are returned
            assert len(result) == 5
            assert result['assay_chembl_id'].tolist() == [f'CHEMBL{i:07d}' for i in range(100, 105)]
    
    def test_duplicate_detection_after_limit(self):
        """Test that duplicate detection works after limit is applied."""
        # Create test data with duplicates
        test_data = pd.DataFrame({
            'assay_chembl_id': ['CHEMBL100', 'CHEMBL101', 'CHEMBL100', 'CHEMBL102']
        })
        
        # Create config with limit of 3
        config = AssayConfig()
        config.runtime.limit = 3
        
        # Create pipeline
        pipeline = AssayPipeline(config)
        
        # Mock the ChEMBL client to avoid API calls
        with patch.object(pipeline, '_setup_clients'):
            
            # Run extract method - should raise ValueError due to duplicates
            with pytest.raises(ValueError, match="Duplicate assay_chembl_id values detected"):
                pipeline.extract(test_data)


if __name__ == "__main__":
    pytest.main([__file__])
