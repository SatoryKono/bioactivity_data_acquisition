"""Tests for testitem ETL pipeline."""

from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from library.testitem.clients import PubChemClient, TestitemChEMBLClient
from library.testitem.config import TestitemConfig
from library.testitem.pipeline import TestitemValidationError, run_testitem_etl


class TestTestitemConfig:
    """Test testitem configuration."""
    
    def test_config_loading(self):
        """Test configuration loading from YAML file."""
        config_path = Path("configs/config_testitem_full.yaml")
        if config_path.exists():
            config = TestitemConfig.from_file(config_path)
            assert config.pipeline_version == "1.1.0"
            assert config.enable_pubchem is True
            assert config.allow_parent_missing is False


class TestTestitemPipeline:
    """Test testitem ETL pipeline."""
    
    def test_input_validation(self):
        """Test input data validation."""
        # Test with valid input
        valid_input = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL25", "CHEMBL153"],
            "molregno": [1, 2]
        })
        
        # Test with missing required fields
        invalid_input = pd.DataFrame({
            "other_field": ["value1", "value2"]
        })
        
        # Create mock config
        config = Mock(spec=TestitemConfig)
        config.pipeline_version = "1.1.0"
        config.enable_pubchem = True
        config.allow_parent_missing = False
        config.runtime.limit = None
        config.sources = {
            "chembl": Mock(),
            "pubchem": Mock()
        }
        config.http.global_.timeout_sec = 60.0
        config.http.global_.headers = {}
        config.http.global_.retries.total = 5
        config.http.global_.retries.backoff_multiplier = 2.0
        config.http.global_.rate_limit = None
        
        # Mock the clients
        with patch('library.testitem.pipeline._create_chembl_client') as mock_chembl_client, \
             patch('library.testitem.pipeline._create_pubchem_client') as mock_pubchem_client:
            
            mock_chembl_client.return_value = Mock(spec=TestitemChEMBLClient)
            mock_pubchem_client.return_value = Mock(spec=PubChemClient)
            
            # Mock the status call
            mock_chembl_client.return_value.get_chembl_status.return_value = {
                "chembl_release": "33.0",
                "status": "ok"
            }
            
            # Mock the extraction
            with patch('library.testitem.pipeline.extract_batch_data') as mock_extract:
                mock_extract.return_value = pd.DataFrame({
                    "molecule_chembl_id": ["CHEMBL25", "CHEMBL153"],
                    "molregno": [1, 2],
                    "pref_name": ["Aspirin", "Ibuprofen"],
                    "source_system": ["ChEMBL", "ChEMBL"],
                    "extracted_at": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"],
                    "chembl_release": ["33.0", "33.0"]
                })
                
                # Test with valid input
                result = run_testitem_etl(config, input_data=valid_input)
                assert len(result.testitems) == 2
                assert result.meta["pipeline_version"] == "1.1.0"
                
                # Test with invalid input
                with pytest.raises(TestitemValidationError):
                    run_testitem_etl(config, input_data=invalid_input)
    
    def test_empty_input(self):
        """Test handling of empty input."""
        config = Mock(spec=TestitemConfig)
        config.pipeline_version = "1.1.0"
        config.enable_pubchem = True
        config.allow_parent_missing = False
        config.runtime.limit = None
        config.sources = {
            "chembl": Mock(),
            "pubchem": Mock()
        }
        config.http.global_.timeout_sec = 60.0
        config.http.global_.headers = {}
        config.http.global_.retries.total = 5
        config.http.global_.retries.backoff_multiplier = 2.0
        config.http.global_.rate_limit = None
        
        empty_input = pd.DataFrame()
        
        with patch('library.testitem.pipeline._create_chembl_client') as mock_chembl_client:
            mock_chembl_client.return_value = Mock(spec=TestitemChEMBLClient)
            mock_chembl_client.return_value.get_chembl_status.return_value = {
                "chembl_release": "33.0",
                "status": "ok"
            }
            
            result = run_testitem_etl(config, input_data=empty_input)
            assert len(result.testitems) == 0
            assert result.meta["row_count"] == 0

    def test_index_column_addition(self):
        """Test that index column is added to the output data."""
        import tempfile

        from library.config import CsvFormatSettings, DeterminismSettings, OutputSettings
        from library.etl.load import write_deterministic_csv
        
        # Create test data without index column
        test_data = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL25", "CHEMBL153", "CHEMBL1"],
            "molregno": [1, 2, 3],
            "pref_name": ["Aspirin", "Ibuprofen", "Acetaminophen"]
        })
        
        # Create determinism settings with index in column_order
        from library.config import SortSettings
        determinism = DeterminismSettings(
            column_order=["index", "molecule_chembl_id", "molregno", "pref_name"],
            sort=SortSettings(
                by=['molecule_chembl_id'],
                ascending=[True],
                na_position='last'
            )
        )
        
        # Write to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_path = Path(tmp_file.name)
        
        # Create output settings
        output = OutputSettings(
            format="csv",
            csv=CsvFormatSettings(),
            data_path=tmp_path,
            qc_report_path=tmp_path.parent / "qc.csv",
            correlation_path=tmp_path.parent / "corr.csv"
        )
        
        try:
            # Write the data
            write_deterministic_csv(
                test_data,
                tmp_path,
                determinism=determinism,
                output=output
            )
            
            # Read back and verify
            result_data = pd.read_csv(tmp_path)
            
            # Check that index column was added
            assert 'index' in result_data.columns
            # Index should reflect original row positions (after sorting by molecule_chembl_id)
            # CHEMBL1 -> index 2, CHEMBL153 -> index 1, CHEMBL25 -> index 0
            expected_indices = [2, 1, 0]  # Based on sorted molecule_chembl_id
            assert result_data['index'].tolist() == expected_indices
            
            # Check that index is the first column
            assert result_data.columns[0] == 'index'
            
            # Check that other columns are preserved
            assert 'molecule_chembl_id' in result_data.columns
            assert 'molregno' in result_data.columns
            assert 'pref_name' in result_data.columns
            
        finally:
            # Clean up
            if tmp_path.exists():
                tmp_path.unlink()


class TestTestitemClients:
    """Test testitem API clients."""
    
    def test_chembl_client_creation(self):
        """Test ChEMBL client creation."""
        from library.config import APIClientConfig, RetrySettings
        
        config = APIClientConfig(
            name="chembl",
            base_url="https://www.ebi.ac.uk/chembl/api/data",
            headers={"Accept": "application/json"},
            timeout=60.0,
            retries=RetrySettings(total=5, backoff_multiplier=2.0),
            rate_limit=None
        )
        
        client = TestitemChEMBLClient(config)
        assert client.base_url == "https://www.ebi.ac.uk/chembl/api/data"
        assert client.timeout == 60.0
    
    def test_pubchem_client_creation(self):
        """Test PubChem client creation."""
        from library.config import APIClientConfig, RetrySettings
        
        config = APIClientConfig(
            name="pubchem",
            base_url="https://pubchem.ncbi.nlm.nih.gov/rest/pug",
            headers={"Accept": "application/json"},
            timeout=45.0,
            retries=RetrySettings(total=8, backoff_multiplier=2.5),
            rate_limit=None
        )
        
        client = PubChemClient(config)
        assert client.base_url == "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
        assert client.timeout == 45.0


if __name__ == "__main__":
    pytest.main([__file__])
