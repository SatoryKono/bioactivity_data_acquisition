"""Tests for the assay ETL pipeline."""

from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pandera
import pytest

from library.assay import AssayChEMBLClient, AssayConfig, run_assay_etl, write_assay_outputs
from library.assay.pipeline import AssayETLResult, AssayValidationError, _extract_assay_data, _normalise_columns, _normalize_assay_fields, _normalize_list_field
from library.schemas.assay_schema import AssayInputSchema, AssayNormalizedSchema


class TestAssayPipeline:
    """Test cases for assay pipeline functionality."""

    def test_normalise_columns_valid_input(self):
        """Test column normalization with valid input."""
        df = pd.DataFrame({
            "assay_chembl_id": [" CHEMBL123 ", "CHEMBL456"],
            "target_chembl_id": [" CHEMBL789 ", "CHEMBL012"]
        })
        
        result = _normalise_columns(df)
        
        assert result["assay_chembl_id"].iloc[0] == "CHEMBL123"
        assert result["assay_chembl_id"].iloc[1] == "CHEMBL456"
        assert result["target_chembl_id"].iloc[0] == "CHEMBL789"
        assert result["target_chembl_id"].iloc[1] == "CHEMBL012"

    def test_normalise_columns_missing_required(self):
        """Test column normalization with missing required columns."""
        df = pd.DataFrame({
            "some_other_column": ["value1", "value2"]
        })
        
        with pytest.raises(AssayValidationError, match="missing required columns"):
            _normalise_columns(df)

    def test_normalize_list_field_list_input(self):
        """Test list field normalization with list input."""
        input_list = ["item1", "item2", "item1", "item3"]
        result = _normalize_list_field(input_list)
        
        assert result == ["item1", "item2", "item3"]  # Sorted and deduplicated

    def test_normalize_list_field_string_input(self):
        """Test list field normalization with string input."""
        input_string = "item1,item2,item1;item3"
        result = _normalize_list_field(input_string)
        
        assert result == ["item1", "item2", "item3"]  # Sorted and deduplicated

    def test_normalize_list_field_json_input(self):
        """Test list field normalization with JSON string input."""
        import json
        input_json = json.dumps(["item1", "item2", "item1"])
        result = _normalize_list_field(input_json)
        
        assert result == ["item1", "item2"]  # Sorted and deduplicated

    def test_normalize_list_field_none_input(self):
        """Test list field normalization with None input."""
        result = _normalize_list_field(None)
        assert result is None

    def test_normalize_assay_fields(self):
        """Test assay field normalization."""
        df = pd.DataFrame({
            "assay_chembl_id": [" CHEMBL123 ", "CHEMBL456"],
            "assay_type": ["B", "F"],
            "description": ["  Test description  ", None],
            "assay_category": [["cat1", "cat2", "cat1"], None],
            "confidence_score": [7, 15]  # Invalid score
        })
        
        result = _normalize_assay_fields(df)
        
        # Check string normalization
        assert result["assay_chembl_id"].iloc[0] == "CHEMBL123"
        assert result["description"].iloc[0] == "Test description"
        
        # Check assay type mapping
        assert result["assay_type_description"].iloc[0] == "Binding"
        assert result["assay_type_description"].iloc[1] == "Functional"
        
        # Check list normalization
        assert result["assay_category"].iloc[0] == ["cat1", "cat2"]

    @patch('library.assay.pipeline.AssayChEMBLClient')
    def test_extract_assay_data_success(self, mock_client_class):
        """Test successful assay data extraction."""
        # Setup mock client
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock successful API response
        mock_response = {
            "assay_chembl_id": "CHEMBL123",
            "assay_type": "B",
            "src_id": 1,
            "description": "Test assay"
        }
        mock_client.fetch_by_assay_id.return_value = mock_response
        
        # Test data
        input_df = pd.DataFrame({
            "assay_chembl_id": ["CHEMBL123", "CHEMBL456"]
        })
        
        config = Mock()
        
        result = _extract_assay_data(mock_client, input_df, config)
        
        assert len(result) == 2
        assert result["assay_chembl_id"].iloc[0] == "CHEMBL123"
        assert result["assay_type"].iloc[0] == "B"
        assert result["src_id"].iloc[0] == 1

    @patch('library.assay.pipeline.AssayChEMBLClient')
    def test_extract_assay_data_with_errors(self, mock_client_class):
        """Test assay data extraction with API errors."""
        # Setup mock client
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock batch extraction to fail, forcing individual requests
        mock_client.fetch_assays_batch.side_effect = Exception("Batch API Error")
        
        # Mock API error for first call, success for second
        mock_client.fetch_by_assay_id.side_effect = [
            Exception("API Error"),
            {"assay_chembl_id": "CHEMBL456", "assay_type": "F"}
        ]
        
        # Test data
        input_df = pd.DataFrame({
            "assay_chembl_id": ["CHEMBL123", "CHEMBL456"]
        })
        
        config = Mock()
        
        result = _extract_assay_data(mock_client, input_df, config)
        
        assert len(result) == 2
        # First record should have default values due to error
        assert result["assay_chembl_id"].iloc[0] == "CHEMBL123"
        assert result["source_system"].iloc[0] == "ChEMBL"  # Default value
        # Second record should be successful
        assert result["assay_chembl_id"].iloc[1] == "CHEMBL456"
        assert result["assay_type"].iloc[1] == "F"

    def test_assay_input_schema_validation(self):
        """Test AssayInputSchema validation."""
        # Valid data
        valid_df = pd.DataFrame({
            "assay_chembl_id": ["CHEMBL123", "CHEMBL456"],
            "target_chembl_id": ["CHEMBL789", None]
        })
        
        # Should not raise exception
        AssayInputSchema.validate(valid_df)
        
        # Invalid data - missing required column
        invalid_df = pd.DataFrame({
            "some_other_column": ["value1", "value2"]
        })
        
        with pytest.raises((pandera.errors.SchemaError, ValueError)):
            AssayInputSchema.validate(invalid_df)

    def test_assay_normalized_schema_validation(self):
        """Test AssayNormalizedSchema validation."""
        # Valid normalized data
        valid_df = pd.DataFrame({
            "source_system": ["ChEMBL", "ChEMBL"],
            "extracted_at": pd.to_datetime(["2023-01-01", "2023-01-02"]),
            "chembl_release": ["33", "33"],
            "assay_chembl_id": ["CHEMBL123", "CHEMBL456"],
            "src_id": [1, 2],
            "src_name": ["Source1", "Source2"],
            "src_assay_id": ["SRC123", "SRC456"],
            "assay_type": ["B", "F"],
            "assay_type_description": ["Binding", "Functional"],
            "bao_format": ["BAO_0000001", "BAO_0000002"],
            "bao_label": ["Label1", "Label2"],
            "assay_category": [["cat1"], ["cat2"]],
            "assay_classifications": [["class1"], ["class2"]],
            "target_chembl_id": ["CHEMBL789", "CHEMBL012"],
            "relationship_type": ["D", "D"],
            "confidence_score": [7, 8],
            "assay_organism": ["Homo sapiens", "Mus musculus"],
            "assay_tax_id": [9606, 10090],
            "assay_cell_type": ["Cell1", "Cell2"],
            "assay_tissue": ["Tissue1", "Tissue2"],
            "assay_strain": ["Strain1", "Strain2"],
            "assay_subcellular_fraction": ["Fraction1", "Fraction2"],
            "description": ["Description1", "Description2"],
            "assay_parameters": [{"param1": "value1"}, {"param2": "value2"}],
            "assay_format": ["Format1", "Format2"],
            "hash_row": ["hash1", "hash2"],
            "hash_business_key": ["key1", "key2"]
        })
        
        # Should not raise exception
        AssayNormalizedSchema.validate(valid_df)

    @patch('library.assay.pipeline._create_api_client')
    def test_run_assay_etl_with_assay_ids(self, mock_create_client):
        """Test running assay ETL with assay IDs."""
        # Setup mock client
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        
        # Mock ChEMBL status
        mock_client.get_chembl_status.return_value = {"chembl_release": "33"}
        
        # Mock assay data extraction
        mock_assay_data = {
            "assay_chembl_id": "CHEMBL123",
            "assay_type": "B",
            "assay_type_description": "Binding",
            "src_id": 1,
            "src_assay_id": "SRC123",
            "bao_format": "BAO_0000001",
            "bao_label": "Label1",
            "assay_category": ["cat1"],
            "assay_classifications": ["class1"],
            "target_chembl_id": "CHEMBL789",
            "relationship_type": "D",
            "confidence_score": 7,
            "assay_organism": "Homo sapiens",
            "assay_tax_id": 9606,
            "assay_cell_type": "Cell1",
            "assay_tissue": "Tissue1",
            "assay_strain": "Strain1",
            "assay_subcellular_fraction": "Fraction1",
            "assay_parameters": {"param1": "value1"},
            "assay_format": "Format1",
            "description": "Test assay",
            "source_system": "ChEMBL",
            "extracted_at": "2023-01-01T00:00:00",
            "hash_row": "hash1",
            "hash_business_key": "key1"
        }
        mock_client.fetch_by_assay_id.return_value = mock_assay_data
        
        # Mock source enrichment
        mock_client.fetch_source_info.return_value = {
            "src_id": 1,
            "src_name": "Test Source",
            "src_short_name": "TS",
            "src_url": "http://example.com"
        }
        
        # Create config
        config = AssayConfig()
        
        # Run ETL
        result = run_assay_etl(
            config=config,
            assay_ids=["CHEMBL123"]
        )
        
        assert isinstance(result, AssayETLResult)
        assert len(result.assays) == 1
        assert result.assays["assay_chembl_id"].iloc[0] == "CHEMBL123"
        assert result.meta["chembl_release"] == "33"

    @patch('library.assay.pipeline._create_api_client')
    def test_run_assay_etl_with_target_id(self, mock_create_client):
        """Test running assay ETL with target ID."""
        # Setup mock client
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        
        # Mock ChEMBL status
        mock_client.get_chembl_status.return_value = {"chembl_release": "33"}
        
        # Mock assay data extraction by target
        mock_assay_data = [{
            "assay_chembl_id": "CHEMBL123",
            "assay_type": "B",
            "assay_type_description": "Binding",
            "src_id": 1,
            "src_assay_id": "SRC123",
            "bao_format": "BAO_0000001",
            "bao_label": "Label1",
            "assay_category": ["cat1"],
            "assay_classifications": ["class1"],
            "target_chembl_id": "CHEMBL789",
            "relationship_type": "D",
            "confidence_score": 7,
            "assay_organism": "Homo sapiens",
            "assay_tax_id": 9606,
            "assay_cell_type": "Cell1",
            "assay_tissue": "Tissue1",
            "assay_strain": "Strain1",
            "assay_subcellular_fraction": "Fraction1",
            "assay_parameters": {"param1": "value1"},
            "assay_format": "Format1",
            "description": "Test assay",
            "source_system": "ChEMBL",
            "extracted_at": "2023-01-01T00:00:00",
            "hash_row": "hash1",
            "hash_business_key": "key1"
        }]
        mock_client.fetch_by_target_id.return_value = mock_assay_data
        
        # Mock source enrichment
        mock_client.fetch_source_info.return_value = {
            "src_id": 1,
            "src_name": "Test Source",
            "src_short_name": "TS",
            "src_url": "http://example.com"
        }
        
        # Create config
        config = AssayConfig()
        
        # Run ETL
        result = run_assay_etl(
            config=config,
            target_chembl_id="CHEMBL231"
        )
        
        assert isinstance(result, AssayETLResult)
        assert len(result.assays) == 1
        assert result.assays["assay_chembl_id"].iloc[0] == "CHEMBL123"
        assert result.meta["chembl_release"] == "33"

    @patch('library.assay.pipeline._create_api_client')
    def test_run_assay_etl_no_input(self, mock_create_client):
        """Test running assay ETL with no input provided."""
        config = AssayConfig()
        
        with pytest.raises(AssayValidationError, match="Either assay_ids or target_chembl_id must be provided"):
            run_assay_etl(config=config)

    @patch('library.assay.pipeline.write_deterministic_csv')
    @patch('library.assay.pipeline._calculate_checksum')
    def test_write_assay_outputs(self, mock_checksum, mock_write_csv):
        """Test writing assay outputs."""
        # Setup mocks
        mock_checksum.return_value = "test_checksum"
        mock_write_csv.return_value = None
        
        # Create test data
        result = AssayETLResult(
            assays=pd.DataFrame({
                "assay_chembl_id": ["CHEMBL123"],
                "assay_type": ["B"]
            }),
            qc=pd.DataFrame([{"metric": "row_count", "value": 1}]),
            meta={"chembl_release": "33", "row_count": 1}
        )
        
        output_dir = Path.cwd() / "test_output"
        date_tag = "20230101"
        config = AssayConfig()
        
        # Mock directory creation
        with patch.object(Path, 'mkdir'):
            with patch('builtins.open', mock_open()):
                output_paths = write_assay_outputs(result, output_dir, date_tag, config)
        
        # Verify output paths (updated to new naming convention)
        expected_paths = ["csv",  "qc", "meta"]
        for path_name in expected_paths:
            assert path_name in output_paths
            assert output_paths[path_name].name.startswith("assays_20230101")


def mock_open():
    """Mock open function for file operations."""
    from unittest.mock import mock_open as _mock_open
    return _mock_open()


class TestAssayChEMBLClient:
    """Test cases for AssayChEMBLClient."""

    @patch('library.assay.client.BaseApiClient._request')
    def test_fetch_by_assay_id_success(self, mock_request):
        """Test successful assay fetch by ID."""
        # Setup mock response
        mock_response = {
            "assay_chembl_id": "CHEMBL123",
            "assay_type": "B",
            "src_id": 1,
            "description": "Test assay"
        }
        mock_request.return_value = mock_response
        
        # Create client
        from library.config import APIClientConfig
        config = APIClientConfig(name="test", base_url="http://test.com")
        client = AssayChEMBLClient(config)
        
        # Test fetch
        result = client.fetch_by_assay_id("CHEMBL123")
        
        assert result["assay_chembl_id"] == "CHEMBL123"
        assert result["assay_type"] == "B"
        assert result["src_id"] == 1
        assert result["source_system"] == "ChEMBL"

    @patch('library.assay.client.BaseApiClient._request')
    def test_fetch_by_assay_id_error(self, mock_request):
        """Test assay fetch by ID with API error."""
        # Setup mock to raise exception
        mock_request.side_effect = Exception("API Error")
        
        # Create client
        from library.config import APIClientConfig
        config = APIClientConfig(name="test", base_url="http://test.com")
        client = AssayChEMBLClient(config)
        
        # Test fetch
        result = client.fetch_by_assay_id("CHEMBL123")
        
        assert result["assay_chembl_id"] == "CHEMBL123"
        assert result["source_system"] == "ChEMBL"  # Default value when error occurs

    def test_parse_list_field(self):
        """Test parsing list fields from API response."""
        from library.config import APIClientConfig
        config = APIClientConfig(name="test", base_url="http://test.com")
        client = AssayChEMBLClient(config)
        
        # Test with list input
        result = client._parse_list_field(["item1", "item2", "item1"])
        assert result == ["item1", "item2"]
        
        # Test with string input
        result = client._parse_list_field("item1,item2,item1")
        assert result == ["item1", "item2"]
        
        # Test with None input
        result = client._parse_list_field(None)
        assert result is None

    def test_calculate_hashes(self):
        """Test hash calculation methods."""
        from library.config import APIClientConfig
        config = APIClientConfig(name="test", base_url="http://test.com")
        client = AssayChEMBLClient(config)
        
        # Test business key hash
        record = {"assay_chembl_id": "CHEMBL123"}
        hash_key = client._calculate_business_key_hash(record)
        assert len(hash_key) == 16
        assert isinstance(hash_key, str)
        
        # Test row hash
        hash_row = client._calculate_row_hash(record)
        assert len(hash_row) == 16
        assert isinstance(hash_row, str)


if __name__ == "__main__":
    pytest.main([__file__])
