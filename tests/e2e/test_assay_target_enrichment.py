"""Test assay target enrichment functionality."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch

from library.assay.pipeline import AssayPipeline
from library.assay.config import AssayConfig


def create_mock_config():
    """Create a properly mocked AssayConfig for testing."""
    config = Mock(spec=AssayConfig)
    
    # Mock source config with http settings
    source_config = Mock()
    source_config.enabled = True
    source_config.http = Mock()
    source_config.http.base_url = "https://www.ebi.ac.uk/chembl/api/data"
    source_config.http.timeout_sec = 60.0
    source_config.http.retries = Mock()
    source_config.http.retries.total = 3
    source_config.http.retries.backoff_multiplier = 2.0
    source_config.http.headers = {}
    
    config.sources = {"chembl": source_config}
    config.runtime = Mock(limit=None, allow_incomplete_sources=False)
    config.http = Mock()
    config.http.global_ = Mock()
    config.http.global_.timeout_sec = 60.0
    config.http.global_.retries = Mock()
    config.http.global_.retries.total = 3
    config.http.global_.retries.backoff_multiplier = 2.0
    config.http.global_.rate_limit = {"max_calls": 10, "period": 1.0}
    config.http.global_.headers = {}
    return config


class TestAssayTargetEnrichment:
    """Test target enrichment in assay pipeline."""
    
    def test_target_enrichment_integration(self):
        """Test that target enrichment works end-to-end."""
        # Mock configuration
        config = create_mock_config()
        
        # Create pipeline
        pipeline = AssayPipeline(config)
        
        # Mock ChEMBL client
        mock_client = Mock()
        pipeline.clients = {"chembl": mock_client}
        
        # Mock assay data
        assay_data = {
            "assay_chembl_id": "CHEMBL1000139",
            "target_chembl_id": "CHEMBL5525",
            "assay_type": "B",
            "description": "Test assay"
        }
        
        # Mock target data
        target_data = {
            "target_chembl_id": "CHEMBL5525",
            "target_organism": "Cavia porcellus",
            "target_tax_id": 10141,
            "target_uniprot_accession": "Q60490",
            "target_isoform": "Isoform 1"
        }
        
        # Configure mocks
        mock_client.fetch_by_assay_id.return_value = assay_data
        mock_client.fetch_by_target_id.return_value = target_data
        
        # Test data
        input_data = pd.DataFrame([{"assay_chembl_id": "CHEMBL1000139"}])
        
        # Run extraction
        result = pipeline._extract_from_chembl(input_data)
        
        # Verify calls
        mock_client.fetch_by_assay_id.assert_called_once_with("CHEMBL1000139")
        mock_client.fetch_by_target_id.assert_called_once_with("CHEMBL5525")
        
        # Verify enrichment
        assert not result.empty
        assert "target_organism" in result.columns
        assert "target_tax_id" in result.columns
        assert "target_uniprot_accession" in result.columns
        assert "target_isoform" in result.columns
        
        # Verify values
        assert result["target_organism"].iloc[0] == "Cavia porcellus"
        assert result["target_tax_id"].iloc[0] == 10141
        assert result["target_uniprot_accession"].iloc[0] == "Q60490"
        assert result["target_isoform"].iloc[0] == "Isoform 1"
    
    def test_target_enrichment_with_multiple_targets(self):
        """Test enrichment with multiple unique targets."""
        config = create_mock_config()
        pipeline = AssayPipeline(config)
        mock_client = Mock()
        pipeline.clients = {"chembl": mock_client}
        
        # Mock multiple assays with different targets
        assay_data_1 = {
            "assay_chembl_id": "CHEMBL1000139",
            "target_chembl_id": "CHEMBL5525",
            "assay_type": "B"
        }
        assay_data_2 = {
            "assay_chembl_id": "CHEMBL1000140", 
            "target_chembl_id": "CHEMBL4153",
            "assay_type": "B"
        }
        
        target_data_1 = {
            "target_chembl_id": "CHEMBL5525",
            "target_organism": "Cavia porcellus",
            "target_tax_id": 10141
        }
        target_data_2 = {
            "target_chembl_id": "CHEMBL4153",
            "target_organism": "Homo sapiens",
            "target_tax_id": 9606
        }
        
        # Configure mocks
        mock_client.fetch_by_assay_id.side_effect = [assay_data_1, assay_data_2]
        mock_client.fetch_by_target_id.side_effect = [target_data_1, target_data_2]
        
        # Test data
        input_data = pd.DataFrame([
            {"assay_chembl_id": "CHEMBL1000139"},
            {"assay_chembl_id": "CHEMBL1000140"}
        ])
        
        # Run extraction
        result = pipeline._extract_from_chembl(input_data)
        
        # Verify both targets were fetched
        assert mock_client.fetch_by_target_id.call_count == 2
        
        # Verify enrichment worked for both
        assert len(result) == 2
        assert result["target_organism"].notna().sum() == 2
    
    def test_target_enrichment_handles_errors(self):
        """Test that target enrichment handles API errors gracefully."""
        config = create_mock_config()
        pipeline = AssayPipeline(config)
        mock_client = Mock()
        pipeline.clients = {"chembl": mock_client}
        
        # Mock assay data
        assay_data = {
            "assay_chembl_id": "CHEMBL1000139",
            "target_chembl_id": "CHEMBL5525",
            "assay_type": "B"
        }
        
        # Mock target fetch failure
        mock_client.fetch_by_assay_id.return_value = assay_data
        mock_client.fetch_by_target_id.side_effect = Exception("API Error")
        
        input_data = pd.DataFrame([{"assay_chembl_id": "CHEMBL1000139"}])
        
        # Run extraction - should not fail
        result = pipeline._extract_from_chembl(input_data)
        
        # Should still return assay data, but without target enrichment
        assert not result.empty
        assert result["assay_chembl_id"].iloc[0] == "CHEMBL1000139"
        # Target fields should be null due to enrichment failure
        if "target_organism" in result.columns:
            assert result["target_organism"].isna().all()
    
    def test_unavailable_fields_are_null(self):
        """Test that fields unavailable in ChEMBL API are properly set to null."""
        config = create_mock_config()
        pipeline = AssayPipeline(config)
        mock_client = Mock()
        pipeline.clients = {"chembl": mock_client}
        
        # Mock assay data with null values for unavailable fields
        assay_data = {
            "assay_chembl_id": "CHEMBL1000139",
            "target_chembl_id": "CHEMBL5525",
            "assay_type": "B",
            # These should be None as they're not available in API
            "bao_endpoint": None,
            "bao_assay_format": None,
            "variant_id": None,
            "is_variant": None,
            "variant_accession": None,
            "assay_parameters_json": None,
            "assay_format": None
        }
        
        mock_client.fetch_by_assay_id.return_value = assay_data
        mock_client.fetch_by_target_id.return_value = {
            "target_chembl_id": "CHEMBL5525",
            "target_organism": "Cavia porcellus"
        }
        
        input_data = pd.DataFrame([{"assay_chembl_id": "CHEMBL1000139"}])
        result = pipeline._extract_from_chembl(input_data)
        
        # Verify unavailable fields are null
        unavailable_fields = [
            "bao_endpoint", "bao_assay_format", "variant_id", 
            "is_variant", "variant_accession", "assay_parameters_json", "assay_format"
        ]
        
        for field in unavailable_fields:
            if field in result.columns:
                assert result[field].isna().all(), f"Field {field} should be null"
    
    def test_logging_of_unavailable_fields(self):
        """Test that unavailable fields are properly logged."""
        config = create_mock_config()
        pipeline = AssayPipeline(config)
        mock_client = Mock()
        pipeline.clients = {"chembl": mock_client}
        
        mock_client.fetch_by_assay_id.return_value = {
            "assay_chembl_id": "CHEMBL1000139",
            "target_chembl_id": "CHEMBL5525"
        }
        mock_client.fetch_by_target_id.return_value = {
            "target_chembl_id": "CHEMBL5525",
            "target_organism": "Cavia porcellus"
        }
        
        input_data = pd.DataFrame([{"assay_chembl_id": "CHEMBL1000139"}])
        
        # This should log information about unavailable fields
        with patch('library.assay.pipeline.logger') as mock_logger:
            result = pipeline._extract_from_chembl(input_data)
            
            # Verify logging calls
            mock_logger.info.assert_any_call("Fields unavailable in ChEMBL API: ['bao_endpoint', 'bao_assay_format', 'bao_assay_type', 'bao_assay_type_label', 'bao_assay_type_uri', 'bao_assay_format_uri', 'bao_assay_format_label', 'bao_endpoint_uri', 'bao_endpoint_label', 'variant_id', 'is_variant', 'variant_accession', 'variant_sequence_accession', 'variant_sequence_mutation', 'variant_mutations', 'variant_text', 'variant_sequence_id', 'variant_organism', 'assay_parameters_json', 'assay_format']")
            mock_logger.info.assert_any_call("These fields are documented as unavailable in ChEMBL API v33+")


if __name__ == "__main__":
    pytest.main([__file__])
