"""Integration tests for target pipeline array serialization."""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from bioetl.config import load_config
from bioetl.pipelines.chembl.target.run import ChemblTargetPipeline


def create_mock_target_data(count: int = 5) -> list[dict[str, object]]:
    """Create mock target data for testing."""
    targets: list[dict[str, object]] = []
    for i in range(count):
        target: dict[str, object] = {
            "target_chembl_id": f"CHEMBL{100 + i}",
            "pref_name": f"Test Target {i + 1}",
            "target_type": "PROTEIN",
            "organism": "Homo sapiens",
            "tax_id": "9606",
            "species_group_flag": 1 if i % 2 == 0 else 0,
        }
        # Add array fields for some targets
        if i == 0:
            target["cross_references"] = [
                {"xref_id": "X1", "xref_name": "N1", "xref_src": "SRC"},
                {"xref_id": "X2", "xref_name": "N2", "xref_src": "SRC"},
            ]
            target["target_components"] = [
                {
                    "component_id": 1,
                    "accession": "P12345",
                    "component_type": "PROTEIN",
                    "organism": "Homo sapiens",
                    "tax_id": 9606,
                    "target_component_synonyms": [
                        {"syn_type": "GENE_SYMBOL", "synonyms": ["EGFR", "ERBB1"]},
                        {"syn_type": "UNIPROT", "synonyms": "P00533"},
                    ],
                }
            ]
        elif i == 1:
            target["cross_references"] = [{"xref_id": "X1", "xref_name": "N1"}]
            target["target_components"] = [
                {
                    "component_id": 1,
                    "accession": "P12345",
                    "target_component_synonyms": [],
                }
            ]
        else:
            target["cross_references"] = None
            target["target_components"] = None
        targets.append(target)
    return targets


def setup_mock_api_client(mock_targets: list[dict[str, object]]) -> MagicMock:
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

    mock_target_response = MagicMock()
    mock_target_response.json.return_value = {
        "page_meta": {"offset": 0, "limit": 25, "count": len(mock_targets), "next": None},
        "targets": mock_targets,
    }
    mock_target_response.status_code = 200
    mock_target_response.headers = {}

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
        elif "/target.json" in url:
            # Return target data for paginate calls
            return mock_target_response
        # Default fallback
        return mock_target_response

    mock_client.get.side_effect = get_side_effect
    return mock_client


@pytest.mark.integration
class TestTargetPipelineSmoke:
    """Smoke tests for target pipeline array serialization."""

    def test_target_pipeline_serializes_array_fields(self, tmp_path: Path) -> None:
        """Test that target pipeline serializes array fields to strings."""
        config_path = (
            Path(__file__).parent.parent.parent
            / "configs"
            / "pipelines"
            / "target"
            / "target_chembl.yaml"
        )
        config = load_config(config_path)

        # Mock API client factory
        mock_targets = create_mock_target_data(count=5)
        mock_client = setup_mock_api_client(mock_targets)
        with patch(
            "bioetl.core.APIClientFactory.for_source", return_value=mock_client
        ):
            pipeline = ChemblTargetPipeline(config, run_id="test_run")

            # Extract a small sample (limit to 5 records)
            config.cli.limit = 5
            config.cli.dry_run = False  # type: ignore[attr-defined]
            df = pipeline.extract()
            df = pipeline.transform(df)

        # Check that serialized array fields are present
        assert "cross_references__flat" in df.columns, "cross_references__flat missing"
        assert "target_components__flat" in df.columns, "target_components__flat missing"
        assert (
            "target_component_synonyms__flat" in df.columns
        ), "target_component_synonyms__flat missing"

        # Check that array fields are strings (not lists)
        for col in [
            "cross_references__flat",
            "target_components__flat",
            "target_component_synonyms__flat",
        ]:
            if col in df.columns:
                series = df[col]
                for value in series.dropna():
                    assert isinstance(value, str), f"{col} should be string, got {type(value)}"
                    # Check pattern: header+rows format (header/row1/row2/...) or empty string
                    if value:
                        assert re.match(
                            r"^[^/]+(/.+)?$", value
                        ), f"{col} should match header+rows pattern, got: {value[:100]}"

    def test_target_pipeline_has_all_required_fields(self, tmp_path: Path) -> None:
        """Test that target pipeline extracts all required scalar fields."""
        config_path = (
            Path(__file__).parent.parent.parent
            / "configs"
            / "pipelines"
            / "target"
            / "target_chembl.yaml"
        )
        config = load_config(config_path)

        # Mock API client factory
        mock_targets = create_mock_target_data(count=5)
        mock_client = setup_mock_api_client(mock_targets)
        with patch(
            "bioetl.core.APIClientFactory.for_source", return_value=mock_client
        ):
            pipeline = ChemblTargetPipeline(config, run_id="test_run")

            # Extract a small sample
            config.cli.limit = 5
            config.cli.dry_run = False  # type: ignore[attr-defined]
            df = pipeline.extract()
            df = pipeline.transform(df)

        # Check for required scalar fields
        required_fields = [
            "target_chembl_id",
            "pref_name",
            "target_type",
            "organism",
            "tax_id",
            "species_group_flag",
        ]

        for field in required_fields:
            assert field in df.columns, f"Required field {field} missing from output"

        # Check for serialized array fields
        serialized_fields = [
            "cross_references__flat",
            "target_components__flat",
            "target_component_synonyms__flat",
        ]

        for field in serialized_fields:
            assert field in df.columns, f"Serialized field {field} missing from output"

    def test_target_pipeline_array_fields_format(self, tmp_path: Path) -> None:
        """Test that array fields follow header+rows format."""
        config_path = (
            Path(__file__).parent.parent.parent
            / "configs"
            / "pipelines"
            / "target"
            / "target_chembl.yaml"
        )
        config = load_config(config_path)

        # Mock API client factory
        mock_targets = create_mock_target_data(count=5)
        mock_client = setup_mock_api_client(mock_targets)
        with patch(
            "bioetl.core.APIClientFactory.for_source", return_value=mock_client
        ):
            pipeline = ChemblTargetPipeline(config, run_id="test_run")

            # Extract a small sample
            config.cli.limit = 5
            config.cli.dry_run = False  # type: ignore[attr-defined]
            df = pipeline.extract()
            df = pipeline.transform(df)

        # Check cross_references format
        if "cross_references__flat" in df.columns:
            cross_refs = df["cross_references__flat"].dropna()
            for value in cross_refs:
                if value:  # Non-empty values
                    # Should have format: header/row1/row2/...
                    parts = value.split("/")
                    assert (
                        len(parts) >= 2
                    ), f"cross_references should have header+rows, got: {value[:100]}"
                    header = parts[0]
                    assert "|" in header, f"Header should contain |, got: {header}"
                    # Check that header contains expected keys
                    assert "xref_id" in header or "xref_name" in header or "xref_src" in header

        # Check target_components format
        if "target_components__flat" in df.columns:
            components = df["target_components__flat"].dropna()
            for value in components:
                if value:  # Non-empty values
                    parts = value.split("/")
                    assert (
                        len(parts) >= 2
                    ), f"target_components should have header+rows, got: {value[:100]}"
                    header = parts[0]
                    assert "|" in header, f"Header should contain |, got: {header}"

        # Check target_component_synonyms format
        if "target_component_synonyms__flat" in df.columns:
            synonyms = df["target_component_synonyms__flat"].dropna()
            for value in synonyms:
                if value:  # Non-empty values
                    parts = value.split("/")
                    assert (
                        len(parts) >= 2
                    ), f"target_component_synonyms should have header+rows, got: {value[:100]}"
                    header = parts[0]
                    assert "|" in header, f"Header should contain |, got: {header}"
                    # Check that header contains expected keys
                    assert "syn_type" in header or "synonyms" in header

    def test_target_pipeline_handles_empty_arrays(self, tmp_path: Path) -> None:
        """Test that target pipeline handles empty arrays correctly."""
        config_path = (
            Path(__file__).parent.parent.parent
            / "configs"
            / "pipelines"
            / "target"
            / "target_chembl.yaml"
        )
        config = load_config(config_path)

        # Create targets with empty arrays
        mock_targets: list[dict[str, object]] = [
            {
                "target_chembl_id": "CHEMBL1",
                "pref_name": "Test Target",
                "target_type": "PROTEIN",
                "organism": "Homo sapiens",
                "tax_id": "9606",
                "species_group_flag": 1,
                "cross_references": [],
                "target_components": [],
            }
        ]

        mock_client = setup_mock_api_client(mock_targets)
        with patch(
            "bioetl.core.APIClientFactory.for_source", return_value=mock_client
        ):
            pipeline = ChemblTargetPipeline(config, run_id="test_run")

            config.cli.limit = 1
            config.cli.dry_run = False  # type: ignore[attr-defined]
            df = pipeline.extract()
            df = pipeline.transform(df)

        # Check that serialized fields exist and are empty strings
        assert "cross_references__flat" in df.columns
        assert "target_components__flat" in df.columns
        assert "target_component_synonyms__flat" in df.columns

        assert df["cross_references__flat"].iloc[0] == ""
        assert df["target_components__flat"].iloc[0] == ""
        assert df["target_component_synonyms__flat"].iloc[0] == ""
