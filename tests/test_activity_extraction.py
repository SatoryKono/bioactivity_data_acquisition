"""Tests for activity data extraction functionality."""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from library.activity import (
    ActivityChEMBLClient,
    ActivityConfig,
    ActivityNormalizer,
    ActivityValidator,
    ActivityQualityFilter
)


class TestActivityConfig:
    """Test ActivityConfig class."""
    
    def test_config_creation(self):
        """Test basic config creation."""
        config = ActivityConfig()
        assert config.chembl_base_url == "https://www.ebi.ac.uk/chembl/api/data"
        assert config.limit == 1000
        assert config.strict_quality is True
    
    def test_config_from_dict(self):
        """Test config creation from dictionary."""
        config_data = {
            'sources': {
                'chembl': {
                    'http': {
                        'base_url': 'https://test.chembl.api',
                        'timeout_sec': 30.0
                    }
                }
            },
            'runtime': {
                'limit': 500,
                'dry_run': True
            }
        }
        
        config = ActivityConfig.from_dict(config_data)
        assert config.chembl_base_url == 'https://test.chembl.api'
        assert config.limit == 500
        assert config.dry_run is True


class TestActivityNormalizer:
    """Test ActivityNormalizer class."""
    
    def setup_method(self):
        """Setup test data."""
        self.normalizer = ActivityNormalizer()
        
        # Sample raw activity data
        self.raw_data = pd.DataFrame([
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
                'activity_comment': None
            },
            {
                'activity_chembl_id': '12346',
                'assay_chembl_id': 'CHEMBL124',
                'molecule_chembl_id': 'CHEMBL457',
                'target_chembl_id': 'CHEMBL790',
                'document_chembl_id': 'CHEMBL102',
                'standard_type': 'Ki',
                'standard_relation': '>',
                'standard_value': 5.0,
                'standard_units': 'μM',
                'data_validity_comment': 'Manually curated',
                'activity_comment': 'inconclusive'
            }
        ])
    
    def test_normalize_activities(self):
        """Test activity normalization."""
        normalized_df = self.normalizer.normalize_activities(self.raw_data)
        
        # Check that normalized fields were added
        assert 'lower_bound' in normalized_df.columns
        assert 'upper_bound' in normalized_df.columns
        assert 'is_censored' in normalized_df.columns
        assert 'quality_flag' in normalized_df.columns
        assert 'quality_reason' in normalized_df.columns
        
        # Check foreign key mapping
        assert 'assay_key' in normalized_df.columns
        assert 'testitem_key' in normalized_df.columns
        
        # Check interval fields for exact values (=)
        exact_record = normalized_df[normalized_df['standard_relation'] == '='].iloc[0]
        assert exact_record['lower_bound'] == 10.5
        assert exact_record['upper_bound'] == 10.5
        assert exact_record['is_censored'] is False
        
        # Check interval fields for censored values (>)
        censored_record = normalized_df[normalized_df['standard_relation'] == '>'].iloc[0]
        assert censored_record['lower_bound'] == 5.0
        assert pd.isna(censored_record['upper_bound'])
        assert censored_record['is_censored'] is True
    
    def test_quality_flags(self):
        """Test quality flag assignment."""
        normalized_df = self.normalizer.normalize_activities(self.raw_data)
        
        # Check quality flags
        exact_record = normalized_df[normalized_df['standard_relation'] == '='].iloc[0]
        assert exact_record['quality_flag'] == 'good'
        
        censored_record = normalized_df[normalized_df['standard_relation'] == '>'].iloc[0]
        assert censored_record['quality_flag'] == 'warning'
        assert 'problematic_activity_comment' in str(censored_record['quality_reason'])


class TestActivityValidator:
    """Test ActivityValidator class."""
    
    def setup_method(self):
        """Setup test data."""
        self.validator = ActivityValidator()
        
        # Sample normalized data
        self.normalized_data = pd.DataFrame([
            {
                'activity_chembl_id': '12345',
                'assay_chembl_id': 'CHEMBL123',
                'molecule_chembl_id': 'CHEMBL456',
                'standard_type': 'IC50',
                'standard_relation': '=',
                'standard_value': 10.5,
                'lower_bound': 10.5,
                'upper_bound': 10.5,
                'is_censored': False,
                'quality_flag': 'good',
                'source_system': 'ChEMBL',
                'retrieved_at': '2024-01-01T12:00:00Z'
            }
        ])
    
    def test_raw_schema_validation(self):
        """Test raw schema validation."""
        # This should pass validation
        validated_df = self.validator.validate_raw_data(self.normalized_data)
        assert len(validated_df) == 1
    
    def test_validation_report(self):
        """Test validation report generation."""
        report = self.validator.get_validation_report(self.normalized_data)
        
        assert 'total_records' in report
        assert 'validation_checks' in report
        assert report['total_records'] == 1
        
        # Check that validation checks were performed
        assert 'duplicates' in report['validation_checks']
        assert 'missing_activity_chembl_id' in report['validation_checks']


class TestActivityQualityFilter:
    """Test ActivityQualityFilter class."""
    
    def setup_method(self):
        """Setup test data."""
        self.quality_filter = ActivityQualityFilter()
        
        # Sample data with mixed quality
        self.test_data = pd.DataFrame([
            {
                'activity_chembl_id': '12345',
                'assay_chembl_id': 'CHEMBL123',
                'molecule_chembl_id': 'CHEMBL456',
                'standard_type': 'IC50',
                'standard_relation': '=',
                'standard_value': 10.5,
                'data_validity_comment': None,
                'activity_comment': None
            },
            {
                'activity_chembl_id': '12346',
                'assay_chembl_id': 'CHEMBL124',
                'molecule_chembl_id': 'CHEMBL457',
                'standard_type': 'UnknownType',
                'standard_relation': '>',
                'standard_value': 5.0,
                'data_validity_comment': 'Manually curated',
                'activity_comment': 'inconclusive'
            },
            {
                'activity_chembl_id': '12347',
                'assay_chembl_id': None,  # Missing required field
                'molecule_chembl_id': 'CHEMBL458',
                'standard_type': 'Ki',
                'standard_relation': '=',
                'standard_value': 2.0,
                'data_validity_comment': None,
                'activity_comment': None
            }
        ])
    
    def test_strict_quality_filter(self):
        """Test strict quality filtering."""
        accepted, rejected = self.quality_filter.apply_strict_quality_filter(self.test_data)
        
        # First record should be accepted (meets all strict criteria)
        assert len(accepted) == 1
        assert accepted.iloc[0]['activity_chembl_id'] == '12345'
        
        # Other records should be rejected
        assert len(rejected) == 2
        assert '12346' in rejected['activity_chembl_id'].values
        assert '12347' in rejected['activity_chembl_id'].values
    
    def test_moderate_quality_filter(self):
        """Test moderate quality filtering."""
        accepted, rejected = self.quality_filter.apply_moderate_quality_filter(self.test_data)
        
        # Should accept more records than strict filter
        assert len(accepted) >= 1
        assert len(rejected) <= 2
    
    def test_quality_statistics(self):
        """Test quality statistics generation."""
        stats = self.quality_filter.get_quality_statistics(self.test_data)
        
        assert 'total_records' in stats
        assert 'missing_data' in stats
        assert 'foreign_key_coverage' in stats
        assert stats['total_records'] == 3


class TestActivityChEMBLClient:
    """Test ActivityChEMBLClient class."""
    
    def setup_method(self):
        """Setup test client."""
        from library.config import APIClientConfig
        
        config = APIClientConfig(
            name='test_chembl',
            base_url='https://test.chembl.api',
            timeout=30.0,
            headers={'Accept': 'application/json'},
            retries={'total': 3, 'backoff_multiplier': 2.0}
        )
        
        self.client = ActivityChEMBLClient(config)
    
    def test_parse_activity(self):
        """Test activity data parsing."""
        sample_activity = {
            'activity_chembl_id': '12345',
            'assay_chembl_id': 'CHEMBL123',
            'molecule_chembl_id': 'CHEMBL456',
            'target_chembl_id': 'CHEMBL789',
            'document_chembl_id': 'CHEMBL101',
            'type': 'IC50',
            'relation': '=',
            'value': 10.5,
            'units': 'nM',
            'standard_type': 'IC50',
            'standard_relation': '=',
            'standard_value': 10.5,
            'standard_units': 'nM',
            'standard_flag': True,
            'pchembl_value': 7.0,
            'data_validity_comment': None,
            'activity_comment': None,
            'bao_endpoint': 'BAO_0000183',
            'bao_format': 'BAO_0000183',
            'bao_label': 'IC50'
        }
        
        parsed = self.client._parse_activity(sample_activity)  # type: ignore[attr-defined]
        
        # Check that all expected fields are present
        expected_fields = [
            'activity_chembl_id', 'assay_chembl_id', 'molecule_chembl_id',
            'published_type', 'published_relation', 'published_value',
            'standard_type', 'standard_relation', 'standard_value',
            'source_system', 'retrieved_at'
        ]
        
        for field in expected_fields:
            assert field in parsed
        
        # Check specific values
        assert parsed['activity_chembl_id'] == '12345'
        assert parsed['published_type'] == 'IC50'
        assert parsed['standard_value'] == 10.5
        assert parsed['source_system'] == 'ChEMBL'


def test_integration():
    """Integration test for the complete pipeline."""
    # Create sample data
    raw_data = pd.DataFrame([
        {
            'activity_chembl_id': '12345',
            'assay_chembl_id': 'CHEMBL123',
            'molecule_chembl_id': 'CHEMBL456',
            'target_chembl_id': 'CHEMBL789',
            'document_chembl_id': 'CHEMBL101',
            'published_type': 'IC50',
            'published_relation': '=',
            'published_value': 10.5,
            'published_units': 'nM',
            'standard_type': 'IC50',
            'standard_relation': '=',
            'standard_value': 10.5,
            'standard_units': 'nM',
            'standard_flag': True,
            'pchembl_value': 7.0,
            'data_validity_comment': None,
            'activity_comment': None,
            'bao_endpoint': 'BAO_0000183',
            'bao_format': 'BAO_0000183',
            'bao_label': 'IC50',
            'source_system': 'ChEMBL',
            'retrieved_at': '2024-01-01T12:00:00Z'
        }
    ])
    
    # Test complete pipeline
    normalizer = ActivityNormalizer()
    validator = ActivityValidator()
    quality_filter = ActivityQualityFilter()
    
    # Normalize
    normalized_df = normalizer.normalize_activities(raw_data)
    assert len(normalized_df) == 1
    
    # Validate
    validated_df = validator.validate_normalized_data(normalized_df)
    assert len(validated_df) == 1
    
    # Apply quality filters
    quality_results = quality_filter.apply_quality_profiles(validated_df)
    assert quality_results['total_records'] == 1
    
    # Check that strict quality accepts the good record
    assert quality_results['strict_quality']['accepted_count'] == 1
    assert quality_results['rejected']['count'] == 0


if __name__ == "__main__":
    pytest.main([__file__])
