"""Integration tests for ChemblTargetPipeline."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from bioetl.config import PipelineConfig
from bioetl.pipelines.chembl.target import ChemblTargetPipeline


@pytest.mark.integration
class TestChemblTargetPipelineIntegration:
    """Integration test suite for ChemblTargetPipeline."""

    def test_pipeline_dry_run(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        tmp_path: Path,
        mock_chembl_api_client: MagicMock,
    ) -> None:
        """Test full pipeline run in dry-run mode."""
        pipeline_config_fixture.cli.dry_run = True  # type: ignore[attr-defined]
        pipeline_config_fixture.materialization.root = str(tmp_path)  # type: ignore[attr-defined]

        with patch("bioetl.core.client_factory.APIClientFactory.for_source", return_value=mock_chembl_api_client):
            pipeline = ChemblTargetPipeline(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]

            # Test extract in dry-run mode
            df_extract = pipeline.extract()  # type: ignore[misc]
            assert df_extract.empty

            # Test transform with empty DataFrame
            df_transform = pipeline.transform(df_extract)  # type: ignore[misc]
            assert df_transform.empty

            # Test validate with empty DataFrame
            df_validate = pipeline.validate(df_transform)  # type: ignore[misc]
            assert df_validate.empty

    def test_pipeline_transform_empty_dataframe(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        mock_chembl_api_client: MagicMock,
    ) -> None:
        """Test transform with empty DataFrame."""
        with patch("bioetl.core.client_factory.APIClientFactory.for_source", return_value=mock_chembl_api_client):
            pipeline = ChemblTargetPipeline(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]

            df = pd.DataFrame()
            result = pipeline.transform(df)  # type: ignore[misc]

            assert result.empty

    def test_pipeline_transform_minimal_dataframe(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        mock_chembl_api_client: MagicMock,
    ) -> None:
        """Test transform with minimal valid DataFrame."""
        with patch("bioetl.core.client_factory.APIClientFactory.for_source", return_value=mock_chembl_api_client):
            pipeline = ChemblTargetPipeline(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]

            df = pd.DataFrame({
                "target_chembl_id": ["CHEMBL1"],
                "pref_name": ["Test Target"],
            })

            # In dry-run mode, enrichment should be skipped
            pipeline_config_fixture.cli.dry_run = True  # type: ignore[attr-defined]
            result = pipeline.transform(df)  # type: ignore[misc]

            assert not result.empty
            assert "target_chembl_id" in result.columns
            assert result["target_chembl_id"].iloc[0] == "CHEMBL1"

