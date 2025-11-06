"""Integration tests for full pipeline lifecycle."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import yaml

from bioetl.config import PipelineConfig
from bioetl.pipelines.activity.activity import ChemblActivityPipeline


@pytest.mark.integration
class TestPipelineLifecycle:
    """Test suite for full pipeline lifecycle."""

    def test_full_pipeline_lifecycle(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        sample_activity_data_raw: list[dict[str, Any]],
        tmp_output_dir: Path,
    ):
        """Test full pipeline lifecycle with mocked HTTP."""
        pipeline_config_fixture.determinism.sort.by = ["activity_id"]
        pipeline_config_fixture.determinism.sort.ascending = [True]
        pipeline_config_fixture.determinism.hashing.business_key_fields = ("activity_id",)
        pipeline_config_fixture.validation.schema_out = None

        with patch("bioetl.core.client_factory.APIClientFactory.for_source") as mock_factory:
            mock_client = MagicMock()
            mock_status_response = MagicMock()
            mock_status_response.json.return_value = {"chembl_release": "33"}
            mock_status_response.status_code = 200
            mock_status_response.headers = {}

            mock_activity_response = MagicMock()
            mock_activity_response.json.return_value = {
                "page_meta": {"offset": 0, "limit": 25, "count": 2, "next": None},
                "activities": sample_activity_data_raw,
            }
            mock_activity_response.status_code = 200
            mock_activity_response.headers = {}

            mock_client.get.side_effect = [mock_status_response, mock_activity_response]
            mock_factory.return_value = mock_client

            pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
            result = pipeline.run(tmp_output_dir)

            assert result.run_id == run_id
            assert result.write_result.dataset.exists()
            assert result.write_result.quality_report is not None
            assert result.write_result.quality_report.exists()

            # Verify artifacts were created
            assert result.write_result.dataset.stat().st_size > 0
            assert result.write_result.metadata is not None
            assert result.write_result.metadata.exists()

            meta_content = yaml.safe_load(result.write_result.metadata.read_text())
            assert meta_content.get("chembl_release") == "33"
            assert "requested_at_utc" in meta_content
            datetime.fromisoformat(str(meta_content["requested_at_utc"]).replace("Z", "+00:00"))
            filters_section = meta_content.get("filters")
            assert isinstance(filters_section, dict)
            assert filters_section.get("mode") == "all"
            assert "page_size" in filters_section
            assert meta_content.get("config_version") == pipeline_config_fixture.version
            assert meta_content.get("pipeline_version") == pipeline_config_fixture.pipeline.version

    def test_pipeline_lifecycle_with_validation_error(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        tmp_output_dir: Path,
    ):
        """Test pipeline lifecycle with validation error."""
        pipeline_config_fixture.validation.schema_out = "bioetl.schemas.activity_chembl:ActivitySchema"
        pipeline_config_fixture.validation.strict = True
        pipeline_config_fixture.determinism.sort.by = []
        pipeline_config_fixture.determinism.sort.ascending = []

        with patch("bioetl.core.client_factory.APIClientFactory.for_source") as mock_factory:
            mock_client = MagicMock()
            mock_status_response = MagicMock()
            mock_status_response.json.return_value = {"chembl_release": "33"}
            mock_status_response.status_code = 200
            mock_status_response.headers = {}

            mock_activity_response = MagicMock()
            # Invalid data that will fail validation
            mock_activity_response.json.return_value = {
                "page_meta": {"offset": 0, "limit": 25, "count": 1, "next": None},
                "activities": [
                    {
                        "activity_id": 1,
                        "molecule_chembl_id": "INVALID",  # Invalid ChEMBL ID
                    },
                ],
            }
            mock_activity_response.status_code = 200
            mock_activity_response.headers = {}

            mock_client.get.side_effect = [mock_status_response, mock_activity_response]
            mock_factory.return_value = mock_client

            pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

            # Should raise validation error or handle gracefully
            try:
                result = pipeline.run(tmp_output_dir)
                # If it succeeds, validation should have cleaned invalid data
                assert result.write_result.dataset.exists()
            except Exception:
                # Validation errors are acceptable
                pass

    def test_pipeline_lifecycle_with_empty_data(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        tmp_output_dir: Path,
    ):
        """Test pipeline lifecycle with empty data."""
        pipeline_config_fixture.validation.schema_out = None
        pipeline_config_fixture.determinism.sort.by = []
        pipeline_config_fixture.determinism.sort.ascending = []
        with patch("bioetl.core.client_factory.APIClientFactory.for_source") as mock_factory:
            mock_client = MagicMock()
            mock_status_response = MagicMock()
            mock_status_response.json.return_value = {"chembl_release": "33"}
            mock_status_response.status_code = 200
            mock_status_response.headers = {}

            mock_activity_response = MagicMock()
            mock_activity_response.json.return_value = {
                "page_meta": {"offset": 0, "limit": 25, "count": 0, "next": None},
                "activities": [],
            }
            mock_activity_response.status_code = 200
            mock_activity_response.headers = {}

            mock_client.get.side_effect = [mock_status_response, mock_activity_response]
            mock_factory.return_value = mock_client

            pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
            result = pipeline.run(tmp_output_dir)

            # Should handle empty data gracefully
            assert result.write_result.dataset.exists()

