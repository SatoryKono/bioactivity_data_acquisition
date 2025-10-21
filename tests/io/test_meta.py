"""Tests for metadata handling module."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import yaml

from library.config import APIClientConfig
from library.io.meta import DatasetMetadata, create_dataset_metadata


class TestDatasetMetadata:
    """Test cases for DatasetMetadata class."""
    
    def test_init(self):
        """Test metadata initialization."""
        metadata = DatasetMetadata("test_dataset")
        assert metadata.dataset == "test_dataset"
        assert metadata._run_id is not None
        assert metadata._generated_at is not None
        assert metadata._chembl_status is None
    
    def test_init_with_logger(self):
        """Test metadata initialization with logger."""
        logger = Mock()
        metadata = DatasetMetadata("test_dataset", logger)
        assert metadata.logger == logger
    
    @patch('library.io.meta.ChEMBLStatusClient')
    def test_get_chembl_status_success(self, mock_client_class):
        """Test successful ChEMBL status retrieval."""
        # Mock client and response
        mock_client = Mock()
        mock_client.get_chembl_status.return_value = {
            "chembl_db_version": "ChEMBL_33",
            "chembl_release_date": "2024-01-15",
            "status": "ok"
        }
        mock_client_class.return_value = mock_client
        
        config = APIClientConfig(
            base_url="https://www.ebi.ac.uk/chembl/api/data",
            timeout=30,
            retries=3
        )
        
        metadata = DatasetMetadata("chembl")
        result = metadata.get_chembl_status(config)
        
        assert result["chembl_db_version"] == "ChEMBL_33"
        assert result["chembl_release_date"] == "2024-01-15"
        assert result["status"] == "ok"
        
        # Test caching - second call should not create new client
        result2 = metadata.get_chembl_status(config)
        assert result == result2
        mock_client_class.assert_called_once()
    
    @patch('library.io.meta.ChEMBLStatusClient')
    def test_get_chembl_status_error(self, mock_client_class):
        """Test ChEMBL status retrieval with error."""
        # Mock client error
        mock_client = Mock()
        mock_client.get_chembl_status.side_effect = Exception("API error")
        mock_client_class.return_value = mock_client
        
        config = APIClientConfig(
            base_url="https://www.ebi.ac.uk/chembl/api/data",
            timeout=30,
            retries=3
        )
        
        metadata = DatasetMetadata("chembl")
        result = metadata.get_chembl_status(config)
        
        assert result["chembl_db_version"] is None
        assert result["chembl_release_date"] is None
        assert result["status"] == "error"
        assert "error" in result
    
    @patch('library.io.meta.ChEMBLStatusClient')
    def test_to_dict(self, mock_client_class):
        """Test metadata to dictionary conversion."""
        # Mock client and response
        mock_client = Mock()
        mock_client.get_chembl_status.return_value = {
            "chembl_db_version": "ChEMBL_33",
            "chembl_release_date": "2024-01-15",
            "status": "ok",
            "timestamp": "2024-01-15T10:00:00Z"
        }
        mock_client_class.return_value = mock_client
        
        config = APIClientConfig(
            base_url="https://www.ebi.ac.uk/chembl/api/data",
            timeout=30,
            retries=3
        )
        
        metadata = DatasetMetadata("chembl")
        result = metadata.to_dict(config)
        
        # Check required fields
        assert result["dataset"] == "chembl"
        assert result["run_id"] is not None
        assert result["generated_at"] is not None
        assert result["chembl_db_version"] == "ChEMBL_33"
        assert result["chembl_release_date"] == "2024-01-15"
        assert result["chembl_status"] == "ok"
        assert result["chembl_status_timestamp"] == "2024-01-15T10:00:00Z"
    
    def test_to_dict_without_config(self):
        """Test metadata to dictionary conversion without config."""
        metadata = DatasetMetadata("test_dataset")
        result = metadata.to_dict()
        
        # Should still have basic fields
        assert result["dataset"] == "test_dataset"
        assert result["run_id"] is not None
        assert result["generated_at"] is not None
        # ChEMBL fields should be None since no config provided
        assert result["chembl_db_version"] is None
        assert result["chembl_release_date"] is None
    
    @patch('library.io.meta.ChEMBLStatusClient')
    def test_save_to_file(self, mock_client_class):
        """Test saving metadata to file."""
        # Mock client and response
        mock_client = Mock()
        mock_client.get_chembl_status.return_value = {
            "chembl_db_version": "ChEMBL_33",
            "chembl_release_date": "2024-01-15",
            "status": "ok"
        }
        mock_client_class.return_value = mock_client
        
        config = APIClientConfig(
            base_url="https://www.ebi.ac.uk/chembl/api/data",
            timeout=30,
            retries=3
        )
        
        metadata = DatasetMetadata("chembl")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "test_meta.yaml"
            metadata.save_to_file(file_path, config)
            
            # Check file was created
            assert file_path.exists()
            
            # Check file contents
            with file_path.open("r", encoding="utf-8") as f:
                loaded_data = yaml.safe_load(f)
            
            assert loaded_data["dataset"] == "chembl"
            assert loaded_data["chembl_db_version"] == "ChEMBL_33"
            assert loaded_data["chembl_release_date"] == "2024-01-15"


class TestCreateDatasetMetadata:
    """Test cases for create_dataset_metadata function."""
    
    def test_create_dataset_metadata(self):
        """Test create_dataset_metadata function."""
        metadata = create_dataset_metadata("test_dataset")
        assert isinstance(metadata, DatasetMetadata)
        assert metadata.dataset == "test_dataset"
    
    def test_create_dataset_metadata_with_config(self):
        """Test create_dataset_metadata function with config."""
        config = APIClientConfig(
            base_url="https://www.ebi.ac.uk/chembl/api/data",
            timeout=30,
            retries=3
        )
        logger = Mock()
        
        metadata = create_dataset_metadata("chembl", config, logger)
        assert isinstance(metadata, DatasetMetadata)
        assert metadata.dataset == "chembl"
        assert metadata.logger == logger
