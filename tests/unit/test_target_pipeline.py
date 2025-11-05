"""Unit tests for ChemblTargetPipeline."""

from __future__ import annotations

from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest

from bioetl.clients.chembl import ChemblClient
from bioetl.config import PipelineConfig
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.pipelines.chembl.target import ChemblTargetPipeline


@pytest.mark.unit
class TestChemblTargetPipeline:
    """Test suite for ChemblTargetPipeline."""

    def test_init(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test ChemblTargetPipeline initialization."""
        pipeline = ChemblTargetPipeline(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]

        assert pipeline.config == pipeline_config_fixture
        assert pipeline.run_id == run_id
        assert pipeline.actor == "target_chembl"
        assert pipeline.chembl_release is None

    def test_chembl_release_property(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test chembl_release property."""
        pipeline = ChemblTargetPipeline(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]

        assert pipeline.chembl_release is None
        pipeline._chembl_release = "33"  # noqa: SLF001  # type: ignore[attr-defined]
        assert pipeline.chembl_release == "33"

    def test_fetch_chembl_release(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test fetching ChEMBL release version."""
        pipeline = ChemblTargetPipeline(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]

        mock_client = Mock(spec=ChemblClient)
        mock_client.handshake.return_value = {"chembl_db_version": "31"}

        from bioetl.core.logger import UnifiedLogger

        log = UnifiedLogger.get(__name__)
        result = pipeline._fetch_chembl_release(mock_client, log)  # noqa: SLF001  # type: ignore[arg-type,attr-defined]

        assert result == "31"

    def test_harmonize_identifier_columns(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test harmonization of identifier column names."""
        pipeline = ChemblTargetPipeline(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]

        df = pd.DataFrame({
            "target_id": ["CHEMBL1", "CHEMBL2"],
            "pref_name": ["Target 1", "Target 2"],
        })

        from bioetl.core.logger import UnifiedLogger

        log = UnifiedLogger.get(__name__)
        result = pipeline._harmonize_identifier_columns(df, log)  # noqa: SLF001  # type: ignore[arg-type]

        assert "target_chembl_id" in result.columns
        assert "target_id" not in result.columns
        assert result["target_chembl_id"].iloc[0] == "CHEMBL1"

    def test_normalize_identifiers(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test normalization of ChEMBL identifiers."""
        pipeline = ChemblTargetPipeline(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]

        df = pd.DataFrame({
            "target_chembl_id": [" CHEMBL1 ", "CHEMBL2", "INVALID", None],
        })

        from bioetl.core.logger import UnifiedLogger

        log = UnifiedLogger.get(__name__)
        result = pipeline._normalize_identifiers(df, log)  # noqa: SLF001  # type: ignore[arg-type]

        assert result["target_chembl_id"].iloc[0] == "CHEMBL1"
        assert result["target_chembl_id"].iloc[1] == "CHEMBL2"
        # Invalid IDs should be set to NA
        assert pd.isna(result["target_chembl_id"].iloc[2])

    def test_normalize_string_fields(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test normalization of string fields."""
        pipeline = ChemblTargetPipeline(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]

        df = pd.DataFrame({
            "pref_name": [" Target 1 ", "Target 2"],
            "organism": [" Human ", "Mouse"],
            "target_type": [" SINGLE PROTEIN ", "PROTEIN COMPLEX"],
        })

        from bioetl.core.logger import UnifiedLogger

        log = UnifiedLogger.get(__name__)
        result = pipeline._normalize_string_fields(df, log)  # noqa: SLF001  # type: ignore[arg-type]

        assert result["pref_name"].iloc[0] == "Target 1"
        assert result["organism"].iloc[0] == "Human"
        assert result["target_type"].iloc[0] == "SINGLE PROTEIN"

    def test_normalize_data_types(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test normalization of data types."""
        pipeline = ChemblTargetPipeline(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]

        df = pd.DataFrame({
            "component_count": ["1", "2", None, "invalid"],
        })

        from bioetl.core.logger import UnifiedLogger

        log = UnifiedLogger.get(__name__)
        result = pipeline._normalize_data_types(df, log)  # noqa: SLF001  # type: ignore[arg-type]

        assert result["component_count"].dtype == "Int64"
        assert result["component_count"].iloc[0] == 1
        assert result["component_count"].iloc[1] == 2
        assert pd.isna(result["component_count"].iloc[2])

    def test_ensure_schema_columns(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test adding missing schema columns."""
        pipeline = ChemblTargetPipeline(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]

        df = pd.DataFrame({
            "target_chembl_id": ["CHEMBL1"],
            "pref_name": ["Target 1"],
        })

        from bioetl.core.logger import UnifiedLogger

        log = UnifiedLogger.get(__name__)
        result = pipeline._ensure_schema_columns(df, log)  # noqa: SLF001  # type: ignore[arg-type]

        # All schema columns should be present
        from bioetl.schemas.target_chembl import COLUMN_ORDER

        for col in COLUMN_ORDER:
            assert col in result.columns

    def test_order_schema_columns(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test reordering columns to match schema order."""
        pipeline = ChemblTargetPipeline(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]

        df = pd.DataFrame({
            "pref_name": ["Target 1"],
            "target_chembl_id": ["CHEMBL1"],
            "target_type": ["SINGLE PROTEIN"],
        })

        result = pipeline._order_schema_columns(df)  # noqa: SLF001  # type: ignore[attr-defined]

        # target_chembl_id should come first (first in COLUMN_ORDER)
        assert result.columns[0] == "target_chembl_id"

    def test_extract_all_dry_run(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test extract_all in dry-run mode."""
        pipeline_config_fixture.cli.dry_run = True  # type: ignore[attr-defined]
        pipeline = ChemblTargetPipeline(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]

        result = pipeline.extract_all()  # type: ignore[misc]

        assert result.empty

    def test_extract_by_ids_dry_run(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test extract_by_ids in dry-run mode."""
        pipeline_config_fixture.cli.dry_run = True  # type: ignore[attr-defined]
        pipeline = ChemblTargetPipeline(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]

        result = pipeline.extract_by_ids(["CHEMBL1", "CHEMBL2"])  # type: ignore[misc]

        assert result.empty

