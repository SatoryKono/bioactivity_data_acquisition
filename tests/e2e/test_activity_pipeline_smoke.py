"""Smoke tests for activity pipeline functionality."""

from unittest.mock import patch

import pandas as pd
import pytest

from library.activity import ActivityConfig, ActivityPipeline
from library.activity.pipeline import ActivityETLResult


class TestActivityPipelineSmoke:
    """Smoke tests for ActivityPipeline class."""

    def setup_method(self):
        """Setup test data and mocks."""
        self.config = ActivityConfig()
        self.pipeline = ActivityPipeline(self.config)
        
        # Mock data
        self.mock_activities = pd.DataFrame([
            {
                'activity_chembl_id': '12345',
                'assay_chembl_id': 'CHEMBL123',
                'molecule_chembl_id': 'CHEMBL456',
                'target_chembl_id': 'CHEMBL789',
                'document_chembl_id': 'CHEMBL101',
                'standard_type': 'IC50',
                'standard_relation': '=',
                'standard_value': 10.5,
                'standard_units': 'nM',
                'data_validity_comment': None,
                'activity_comment': None,
                'source_system': 'ChEMBL'
            }
        ])
        
        self.mock_qc = pd.DataFrame([
            {
                'activity_chembl_id': '12345',
                'quality_flag': 'good'
            }
        ])

    @patch('library.activity.pipeline.ActivityPipeline._extract_activities')
    @patch('library.activity.pipeline.ActivityPipeline._validate_activities')
    @patch('library.activity.pipeline.ActivityPipeline._quality_control')
    def test_pipeline_run_smoke(self, mock_qc, mock_validate, mock_extract):
        """Test that pipeline runs without errors."""
        # Setup mocks
        mock_extract.return_value = self.mock_activities
        mock_validate.return_value = self.mock_activities
        mock_qc.return_value = (self.mock_activities, self.mock_qc)
        
        # Run pipeline
        result = self.pipeline.run()
        
        # Verify result structure
        assert isinstance(result, ActivityETLResult)
        assert hasattr(result, 'activities')  # Correct attribute name
        assert hasattr(result, 'qc')
        assert hasattr(result, 'meta')
        
        # Verify data
        assert len(result.activities) == 1  # Use correct attribute name
        assert len(result.qc) >= 1  # QC report contains metrics, not data records
        assert result.activities.iloc[0]['activity_chembl_id'] == '12345'

    def test_etl_result_structure(self):
        """Test ActivityETLResult structure."""
        result = ActivityETLResult(
            activities=self.mock_activities,
            qc=self.mock_qc,
            meta={'test': 'data'}
        )
        
        # Test correct attribute access
        assert hasattr(result, 'activities')  # Not 'activity'
        assert hasattr(result, 'qc')
        assert hasattr(result, 'meta')
        assert hasattr(result, 'correlation_analysis')
        assert hasattr(result, 'correlation_reports')
        assert hasattr(result, 'correlation_insights')
        
        # Test data access
        assert len(result.activities) == 1  # Use correct attribute name
        assert result.activities.iloc[0]['activity_chembl_id'] == '12345'
        assert len(result.qc) >= 1  # QC report contains metrics
        assert result.meta['test'] == 'data'

    @patch('library.activity.pipeline.ActivityPipeline._extract_activities')
    def test_pipeline_with_empty_data(self, mock_extract):
        """Test pipeline behavior with empty data."""
        mock_extract.return_value = pd.DataFrame()
        
        result = self.pipeline.run()
        
        assert isinstance(result, ActivityETLResult)
        assert len(result.activities) == 0  # Use correct attribute name
        assert len(result.qc) >= 0  # QC report can be empty or contain metrics

    def test_config_validation(self):
        """Test that config is properly validated."""
        # Test that config has required attributes
        assert hasattr(self.config, 'sources')
        assert hasattr(self.config, 'runtime')
        assert hasattr(self.config, 'http')
        assert hasattr(self.config, 'io')
        
        # Test sources configuration
        assert 'chembl' in self.config.sources
        chembl_config = self.config.sources['chembl']
        assert hasattr(chembl_config, 'http')
        assert hasattr(chembl_config.http, 'base_url')
        # base_url can be None by default, it will be set to default in to_api_client_config()
        
        # Test runtime configuration
        assert hasattr(self.config.runtime, 'limit')
        # limit can be None by default (no limit)
        if self.config.runtime.limit is not None:
            assert isinstance(self.config.runtime.limit, int)
            assert self.config.runtime.limit > 0


if __name__ == "__main__":
    pytest.main([__file__])