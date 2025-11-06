"""Integration tests for assay pipeline array serialization."""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from bioetl.config import load_config
from bioetl.pipelines.assay.assay import ChemblAssayPipeline


def create_mock_assay_data(count: int = 5) -> list[dict[str, object]]:
    """Create mock assay data for testing."""
    assays: list[dict[str, object]] = []
    for i in range(count):
        assay: dict[str, object] = {
            "assay_chembl_id": f"CHEMBL{100 + i}",
            "description": f"Test assay {i + 1}",
            "assay_type": "B" if i % 2 == 0 else "F",
            "assay_type_description": "Binding" if i % 2 == 0 else "Functional",
            "assay_test_type": "In vitro",
            "assay_category": "ADMET",
            "assay_organism": "Homo sapiens",
            "assay_tax_id": 9606,
            "assay_strain": None,
            "assay_tissue": "Whole organism",
            "assay_cell_type": None,
            "assay_subcellular_fraction": None,
            "target_chembl_id": f"CHEMBL{200 + i}",
            "document_chembl_id": f"CHEMBL{300 + i}",
            "src_id": i + 1,
            "src_assay_id": f"SRC{i + 1}",
            "cell_chembl_id": None,
            "tissue_chembl_id": f"CHEMBL{400 + i}",
            "assay_group": "Group 1",
            "confidence_score": 8,
            "confidence_description": "High confidence",
            "variant_sequence": None,
        }
        # Add array fields for some assays
        if i == 0:
            assay["assay_classifications"] = [
                {"assay_class_id": "BAO_0000015", "class_name": "Binding"},
                {"assay_class_id": "BAO_0000016", "class_name": "Functional"},
            ]
            assay["assay_parameters"] = [
                {"parameter_name": "Kd", "parameter_value": "10.5"},
                {"parameter_name": "IC50", "parameter_value": "20.3"},
            ]
        elif i == 1:
            assay["assay_classifications"] = [{"assay_class_id": "BAO_0000015"}]
            assay["assay_parameters"] = []
        else:
            assay["assay_classifications"] = None
            assay["assay_parameters"] = None
        assays.append(assay)
    return assays


def setup_mock_api_client(mock_assays: list[dict[str, object]]) -> MagicMock:
    """Setup mock API client and factory for testing."""
    mock_client = MagicMock()

    # Create responses
    mock_status_json_response = MagicMock()
    mock_status_json_response.json.return_value = {"chembl_db_version": "33", "api_version": "1.0"}
    mock_status_json_response.status_code = 200
    mock_status_json_response.headers = {}

    mock_status_response = MagicMock()
    mock_status_response.json.return_value = {"chembl_db_version": "33", "api_version": "1.0"}
    mock_status_response.status_code = 200
    mock_status_response.headers = {}

    mock_assay_response = MagicMock()
    mock_assay_response.json.return_value = {
        "page_meta": {"offset": 0, "limit": 25, "count": len(mock_assays), "next": None},
        "assays": mock_assays,
    }
    mock_assay_response.status_code = 200
    mock_assay_response.headers = {}

    # Use a function to handle multiple calls - return appropriate response based on URL
    call_count = {"count": 0}
    responses = [mock_status_json_response, mock_status_response]

    def get_side_effect(url: str, *args: object, **kwargs: object) -> MagicMock:
        call_count["count"] += 1
        if "/status" in url or "/status.json" in url:
            # Return status response for handshake calls
            if call_count["count"] <= len(responses):
                return responses[call_count["count"] - 1]
            return mock_status_response  # Default for subsequent calls
        elif "/assay.json" in url:
            # Return assay data for paginate calls
            return mock_assay_response
        # Default fallback
        return mock_assay_response

    mock_client.get.side_effect = get_side_effect
    return mock_client


@pytest.mark.integration
class TestAssayPipelineSmoke:
    """Smoke tests for assay pipeline array serialization."""

    def test_assay_pipeline_serializes_array_fields(self, tmp_path: Path) -> None:
        """Test that assay pipeline serializes array fields to strings."""
        config_path = Path(__file__).parent.parent.parent / "configs" / "pipelines" / "chembl" / "assay.yaml"
        config = load_config(config_path)

        # Mock API client factory
        mock_assays = create_mock_assay_data(count=5)
        mock_client = setup_mock_api_client(mock_assays)
        with patch("bioetl.core.client_factory.APIClientFactory.for_source", return_value=mock_client):
            pipeline = ChemblAssayPipeline(config, run_id="test_run")

            # Extract a small sample (limit to 5 records)
            config.cli.limit = 5
            df = pipeline.extract()
            df = pipeline.transform(df)

        # Check that array fields are present
        assert "assay_classifications" in df.columns or "assay_parameters" in df.columns, "Array fields missing"

        # Check that array fields are strings (not lists)
        if "assay_classifications" in df.columns:
            classifications = df["assay_classifications"]
            for value in classifications.dropna():
                assert isinstance(value, str), f"assay_classifications should be string, got {type(value)}"
                # Check pattern: header+rows format (header/row1/row2/...)
                # Empty string is valid, or should match pattern ^[^/]+(/.+)?$
                if value:
                    assert re.match(
                        r"^[^/]+(/.+)?$", value
                    ), f"assay_classifications should match header+rows pattern, got: {value[:100]}"

        if "assay_parameters" in df.columns:
            parameters = df["assay_parameters"]
            for value in parameters.dropna():
                assert isinstance(value, str), f"assay_parameters should be string, got {type(value)}"
                # Check pattern: header+rows format
                if value:
                    assert re.match(
                        r"^[^/]+(/.+)?$", value
                    ), f"assay_parameters should match header+rows pattern, got: {value[:100]}"

    def test_assay_pipeline_has_all_required_fields(self, tmp_path: Path) -> None:
        """Test that assay pipeline extracts all required scalar fields."""
        config_path = Path(__file__).parent.parent.parent / "configs" / "pipelines" / "chembl" / "assay.yaml"
        config = load_config(config_path)

        # Mock API client factory
        mock_assays = create_mock_assay_data(count=5)
        mock_client = setup_mock_api_client(mock_assays)
        with patch("bioetl.core.client_factory.APIClientFactory.for_source", return_value=mock_client):
            pipeline = ChemblAssayPipeline(config, run_id="test_run")

            # Extract a small sample
            config.cli.limit = 5
            df = pipeline.extract()
            df = pipeline.transform(df)

        # Check for required scalar fields
        required_fields = [
            "assay_chembl_id",
            "description",
            "assay_type",
            "assay_type_description",
            "assay_test_type",
            "assay_category",
            "assay_organism",
            "assay_tax_id",
            "assay_strain",
            "assay_tissue",
            "assay_cell_type",
            "assay_subcellular_fraction",
            "target_chembl_id",
            "document_chembl_id",
            "src_id",
            "src_assay_id",
            "cell_chembl_id",
            "tissue_chembl_id",
            "assay_group",
            "confidence_score",
            "confidence_description",
            "variant_sequence",
        ]

        for field in required_fields:
            assert field in df.columns, f"Required field {field} missing from output"

    def test_assay_pipeline_array_fields_format(self, tmp_path: Path) -> None:
        """Test that array fields follow header+rows format."""
        config_path = Path(__file__).parent.parent.parent / "configs" / "pipelines" / "chembl" / "assay.yaml"
        config = load_config(config_path)

        # Mock API client factory
        mock_assays = create_mock_assay_data(count=10)
        mock_client = setup_mock_api_client(mock_assays)
        with patch("bioetl.core.client_factory.APIClientFactory.for_source", return_value=mock_client):
            pipeline = ChemblAssayPipeline(config, run_id="test_run")

            # Extract a small sample
            config.cli.limit = 10
            df = pipeline.extract()
            df = pipeline.transform(df)

        # Check format of array fields
        if "assay_classifications" in df.columns:
            classifications = df["assay_classifications"].dropna()
            for value in classifications:
                if value:  # Non-empty string
                    # Should have format: header/row1/row2/...
                    parts = value.split("/", 1)
                    assert len(parts) >= 1, f"Should have at least header, got: {value[:100]}"
                    header = parts[0]
                    # Header should have keys (pipe-separated if multiple, or single key)
                    assert len(header) > 0, f"Header should not be empty, got: {header}"

        if "assay_parameters" in df.columns:
            parameters = df["assay_parameters"].dropna()
            for value in parameters:
                if value:  # Non-empty string
                    # Should have format: header/row1/row2/...
                    parts = value.split("/", 1)
                    assert len(parts) >= 1, f"Should have at least header, got: {value[:100]}"
                    header = parts[0]
                    # Header should have keys (pipe-separated if multiple, or single key)
                    assert len(header) > 0, f"Header should not be empty, got: {header}"

