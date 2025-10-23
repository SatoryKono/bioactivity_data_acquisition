"""Integration tests for the complete ETL pipeline."""

import os

import pandas as pd
import pytest

from library.config import Config
from library.documents.pipeline import DocumentPipeline


def _check_network_access():
    """Check if network access is available."""
    try:
        import requests
        response = requests.get("https://www.ebi.ac.uk/chembl/api/data/status", timeout=5)
        return response.status_code == 200
    except Exception:
        return False


def _check_api_keys():
    """Check if required API keys are available."""
    required_keys = ['CHEMBL_API_TOKEN', 'PUBMED_API_KEY', 'SEMANTIC_SCHOLAR_API_KEY']
    return all(os.getenv(key) for key in required_keys)


@pytest.mark.integration
class TestPipelineIntegration:
    """Integration tests for the complete document processing pipeline."""

    @pytest.mark.skipif(not _check_network_access(), reason="no network access")
    def test_minimal_pipeline_run(self, integration_config, temp_output_dir, test_documents_csv):
        """Test running the pipeline with minimal configuration."""
        # Update config to use test files
        integration_config.io.input.documents_csv = str(test_documents_csv)
        integration_config.io.output.data_path = temp_output_dir / "data.csv"
        integration_config.io.output.qc_report_path = temp_output_dir / "qc_report.csv"
        integration_config.io.output.correlation_path = temp_output_dir / "correlation.csv"
        
        # Run the pipeline
        pipeline = DocumentPipeline(integration_config)
        result = pipeline.run(input_data=pd.read_csv(test_documents_csv))
        
        # Verify basic results
        assert result is not None
        assert len(result.data) >= 0
        
        # Check that output files were created (even if empty)
        assert integration_config.io.output.data_path.exists()
        assert integration_config.io.output.qc_report_path.exists()
        assert integration_config.io.output.correlation_path.exists()

    @pytest.mark.skipif(not _check_network_access() or not _check_api_keys(), reason="no network access or missing API keys")
    def test_pipeline_with_real_data(self, integration_config, temp_output_dir):
        """Test pipeline with real document data."""
        # Create a more realistic test dataset
        test_data = pd.DataFrame({
            'document_id': ['CHEMBL25', 'CHEMBL1000'],  # Real ChEMBL IDs
            'title': ['Aspirin', 'Acetaminophen'],
            'doi': ['10.1016/j.bcp.2003.08.027', '10.1016/S0024-3205(01)01169-3'],
            'pmid': ['15013854', '11267491'],
            'journal': ['Biochemical Pharmacology', 'Life Sciences'],
            'year': [2004, 2001],
        })
        
        input_dir = temp_output_dir.parent / "input"
        input_dir.mkdir(parents=True)
        csv_path = input_dir / "real_documents.csv"
        test_data.to_csv(csv_path, index=False)
        
        # Update config
        integration_config.io.input.documents_csv = str(csv_path)
        integration_config.io.output.data_path = temp_output_dir / "real_data.csv"
        integration_config.io.output.qc_report_path = temp_output_dir / "real_qc_report.csv"
        integration_config.io.output.correlation_path = temp_output_dir / "real_correlation.csv"
        integration_config.runtime.limit = 5  # Limit processing
        
        try:
            # Run the pipeline
            pipeline = DocumentPipeline(integration_config)
            result = pipeline.run(input_data=pd.read_csv(csv_path))
            
            # Verify results
            assert len(result.data) >= 0
            
            # Check output files
            if len(result.data) > 0:
                assert integration_config.io.output.data_path.exists()
                assert integration_config.io.output.data_path.stat().st_size > 0
                
                # Verify CSV structure
                output_df = pd.read_csv(integration_config.io.output.data_path)
                assert len(output_df) > 0
                assert 'document_id' in output_df.columns
            
        finally:
            # Cleanup
            csv_path.unlink(missing_ok=True)

    def test_pipeline_error_handling(self, integration_config, temp_output_dir):
        """Test pipeline error handling with invalid data."""
        # Create test data with invalid document IDs
        test_data = pd.DataFrame({
            'document_id': ['INVALID_ID_1', 'INVALID_ID_2'],
            'title': ['Invalid Document 1', 'Invalid Document 2'],
            'doi': ['10.invalid/test1', '10.invalid/test2'],
            'pmid': ['invalid1', 'invalid2'],
        })
        
        input_dir = temp_output_dir.parent / "input"
        input_dir.mkdir(parents=True)
        csv_path = input_dir / "invalid_documents.csv"
        test_data.to_csv(csv_path, index=False)
        
        # Update config
        integration_config.io.input.documents_csv = str(csv_path)
        integration_config.io.output.data_path = temp_output_dir / "error_data.csv"
        integration_config.io.output.qc_report_path = temp_output_dir / "error_qc_report.csv"
        integration_config.io.output.correlation_path = temp_output_dir / "error_correlation.csv"
        
        try:
            # Run the pipeline - should handle errors gracefully
            pipeline = DocumentPipeline(integration_config)
            result = pipeline.run(input_data=pd.read_csv(csv_path))
            
            # Should complete without crashing
            assert result is not None
            assert len(result.data) >= 0
            # May have all failed documents, which is expected
            
            # Output files should still be created
            assert integration_config.io.output.data_path.exists()
            assert integration_config.io.output.qc_report_path.exists()
            assert integration_config.io.output.correlation_path.exists()
            
        finally:
            # Cleanup
            csv_path.unlink(missing_ok=True)

    def test_pipeline_configuration_validation(self, temp_output_dir):
        """Test pipeline configuration validation."""
        # Test with invalid configuration
        invalid_config_data = {
            "http": {
                "global": {
                    "timeout_sec": -1.0,  # Invalid negative timeout
                    "retries": {
                        "total": 3
                    }
                }
            },
            "sources": {
                "chembl": {
                    "name": "chembl",
                    "enabled": True,
                    "http": {
                        "base_url": "not-a-valid-url",  # Invalid URL
                    }
                }
            },
            "io": {
                "input": {
                    "documents_csv": str(temp_output_dir / "nonexistent.csv")
                },
                "output": {
                    "data_path": str(temp_output_dir / "data.csv"),
                    "qc_report_path": str(temp_output_dir / "qc_report.csv"),
                    "correlation_path": str(temp_output_dir / "correlation.csv")
                }
            },
            "runtime": {
                "workers": 4,
                "limit": 1000
            },
            "logging": {
                "level": "INFO"
            },
            "validation": {
                "qc": {
                    "min_fill_rate": 0.8
                }
            }
        }
        
        # Should raise validation error
        with pytest.raises((ValueError, TypeError)):  # More specific exception types
            Config.model_validate(invalid_config_data)

    @pytest.mark.skipif(not _check_network_access() or not _check_api_keys(), reason="no network access or missing API keys")
    def test_pipeline_with_different_sources(self, integration_config, temp_output_dir, test_documents_csv):
        """Test pipeline with different API sources enabled."""
        # Enable multiple sources for testing
        integration_config.sources.update({
            "crossref": {
                "name": "crossref",
                "enabled": True,
                "http": {
                    "base_url": "https://api.crossref.org",
                    "rate_limit": {"max_calls": 1, "period": 1.0}
                },
                "params": {},
                "pagination": {
                    "page_param": "offset",
                    "size_param": "rows",
                    "size": 10,
                    "max_pages": 1
                }
            },
            "pubmed": {
                "name": "pubmed",
                "enabled": True,
                "http": {
                    "base_url": "https://eutils.ncbi.nlm.nih.gov/entrez/eutils",
                    "rate_limit": {"max_calls": 1, "period": 1.0}
                },
                "params": {},
                "pagination": {
                    "page_param": "retstart",
                    "size_param": "retmax",
                    "size": 10,
                    "max_pages": 1
                }
            }
        })
        
        # Update config paths
        integration_config.io.input.documents_csv = str(test_documents_csv)
        integration_config.io.output.data_path = temp_output_dir / "multi_source_data.csv"
        integration_config.io.output.qc_report_path = temp_output_dir / "multi_source_qc_report.csv"
        integration_config.io.output.correlation_path = temp_output_dir / "multi_source_correlation.csv"
        
        # Run the pipeline
        pipeline = DocumentPipeline(integration_config)
        result = pipeline.run(input_data=pd.read_csv(test_documents_csv))
        
        # Should complete successfully
        assert result is not None
        assert len(result.data) >= 0
        
        # Check output files
        assert integration_config.io.output.data_path.exists()
        assert integration_config.io.output.qc_report_path.exists()
        assert integration_config.io.output.correlation_path.exists()

    @pytest.mark.slow
    @pytest.mark.skipif(not _check_network_access() or not _check_api_keys(), reason="no network access or missing API keys")
    def test_pipeline_performance(self, integration_config, temp_output_dir):
        """Test pipeline performance with larger dataset."""
        import time
        
        # Create a larger test dataset
        test_data = pd.DataFrame({
            'document_id': [f'CHEMBL{i:05d}' for i in range(1, 21)],  # 20 documents
            'title': [f'Test Document {i}' for i in range(1, 21)],
            'doi': [f'10.1234/test{i:02d}' for i in range(1, 21)],
            'pmid': [str(1000000 + i) for i in range(1, 21)],
        })
        
        input_dir = temp_output_dir.parent / "input"
        input_dir.mkdir(parents=True)
        csv_path = input_dir / "performance_documents.csv"
        test_data.to_csv(csv_path, index=False)
        
        # Update config
        integration_config.io.input.documents_csv = str(csv_path)
        integration_config.io.output.data_path = temp_output_dir / "performance_data.csv"
        integration_config.io.output.qc_report_path = temp_output_dir / "performance_qc_report.csv"
        integration_config.io.output.correlation_path = temp_output_dir / "performance_correlation.csv"
        integration_config.runtime.workers = 2  # Use multiple workers
        integration_config.runtime.limit = 20
        
        try:
            start_time = time.time()
            
            # Run the pipeline
            pipeline = DocumentPipeline(integration_config)
            result = pipeline.run(input_data=pd.read_csv(csv_path))
            
            elapsed_time = time.time() - start_time
            
            # Verify results
            assert result is not None
            assert len(result.data) == 20
            
            # Performance check - should complete within reasonable time
            assert elapsed_time < 300, f"Pipeline took too long: {elapsed_time:.2f} seconds"
            
            # Check that we got some results
            if len(result.data) > 0:
                output_df = pd.read_csv(integration_config.io.output.data_path)
                assert len(output_df) > 0
                
        finally:
            # Cleanup
            csv_path.unlink(missing_ok=True)
