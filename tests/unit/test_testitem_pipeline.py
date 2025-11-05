"""Unit tests for TestItemChemblPipeline."""

from __future__ import annotations

from unittest.mock import Mock

import pandas as pd
import pytest

from bioetl.config import PipelineConfig
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.pipelines.chembl.testitem import TestItemChemblPipeline


@pytest.mark.unit
class TestTestItemChemblPipeline:
    """Test suite for TestItemChemblPipeline."""

    def test_init(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test TestItemChemblPipeline initialization."""
        pipeline = TestItemChemblPipeline(config=pipeline_config_fixture, run_id=run_id)

        assert pipeline.config == pipeline_config_fixture
        assert pipeline.run_id == run_id
        assert pipeline.actor == "testitem_chembl"
        assert pipeline._chembl_db_version is None  # noqa: SLF001  # type: ignore[attr-defined]
        assert pipeline._api_version is None  # noqa: SLF001  # type: ignore[attr-defined]

    def test_fetch_chembl_versions(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test fetching ChEMBL versions from status endpoint."""
        pipeline = TestItemChemblPipeline(config=pipeline_config_fixture, run_id=run_id)

        mock_client = Mock(spec=UnifiedAPIClient)
        mock_response = Mock()
        mock_response.json.return_value = {
            "chembl_db_version": "31",
            "api_version": "1.0.0",
        }
        mock_client.get.return_value = mock_response

        from bioetl.core.logger import UnifiedLogger

        log = UnifiedLogger.get(__name__)
        pipeline._fetch_chembl_versions(mock_client, log)  # noqa: SLF001  # type: ignore[arg-type,attr-defined]

        assert pipeline._chembl_db_version == "31"  # noqa: SLF001  # type: ignore[attr-defined]
        assert pipeline._api_version == "1.0.0"  # noqa: SLF001  # type: ignore[attr-defined]

    def test_flatten_nested_structures(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test flattening of nested molecule_structures and molecule_properties."""
        pipeline = TestItemChemblPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1", "CHEMBL2"],
            "molecule_structures": [
                {"canonical_smiles": "CCO", "standard_inchi_key": "LFQSCWFLJHTTHZ-UHFFFAOYSA-N"},
                {"canonical_smiles": "CC", "standard_inchi_key": None},
            ],
            "molecule_properties": [
                {"full_mwt": 46.07, "alogp": 0.31, "hbd": 1},
                {"full_mwt": 30.07, "alogp": 0.64, "hbd": 0},
            ],
        })

        from bioetl.core.logger import UnifiedLogger

        log = UnifiedLogger.get(__name__)
        result = pipeline._flatten_nested_structures(df, log)  # noqa: SLF001  # type: ignore[arg-type]

        assert "canonical_smiles" in result.columns
        assert "standard_inchi_key" in result.columns
        assert "full_mwt" in result.columns
        assert "alogp" in result.columns
        assert "hbd" in result.columns
        assert result["canonical_smiles"].iloc[0] == "CCO"
        assert result["full_mwt"].iloc[0] == 46.07

    def test_normalize_identifiers(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test normalization of ChEMBL identifiers and InChI keys."""
        pipeline = TestItemChemblPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame({
            "molecule_chembl_id": [" CHEMBL1 ", "CHEMBL2"],
            "standard_inchi_key": [" lfqscwfljhtthz-uhfffaoysa-n ", ""],
        })

        from bioetl.core.logger import UnifiedLogger

        log = UnifiedLogger.get(__name__)
        result = pipeline._normalize_identifiers(df, log)  # noqa: SLF001  # type: ignore[arg-type]

        assert result["molecule_chembl_id"].iloc[0] == "CHEMBL1"
        assert result["standard_inchi_key"].iloc[0] == "LFQSCWFLJHTTHZ-UHFFFAOYSA-N"
        assert pd.isna(result["standard_inchi_key"].iloc[1])

    def test_normalize_string_fields(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test normalization of string fields."""
        pipeline = TestItemChemblPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame({
            "pref_name": [" Ethanol ", " Methane"],
            "canonical_smiles": [" CCO ", ""],
        })

        from bioetl.core.logger import UnifiedLogger

        log = UnifiedLogger.get(__name__)
        result = pipeline._normalize_string_fields(df, log)  # noqa: SLF001  # type: ignore[arg-type]

        assert result["pref_name"].iloc[0] == "Ethanol"
        assert result["canonical_smiles"].iloc[0] == "CCO"
        assert pd.isna(result["canonical_smiles"].iloc[1])

    def test_deduplicate_molecules(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test deduplication of molecules."""
        pipeline = TestItemChemblPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3"],
            "standard_inchi_key": ["KEY1", "KEY1", "KEY2"],
            "canonical_smiles": ["CCO", "CCO", "CC"],
            "full_mwt": [46.07, 46.07, 30.07],
        })

        from bioetl.core.logger import UnifiedLogger

        log = UnifiedLogger.get(__name__)
        result = pipeline._deduplicate_molecules(df, log)  # noqa: SLF001  # type: ignore[arg-type]

        assert len(result) == 2
        assert "CHEMBL1" in result["molecule_chembl_id"].values
        assert "CHEMBL3" in result["molecule_chembl_id"].values

    def test_transform_with_nested_data(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test transform with nested ChEMBL data."""
        pipeline = TestItemChemblPipeline(config=pipeline_config_fixture, run_id=run_id)
        pipeline._chembl_db_version = "31"  # noqa: SLF001  # type: ignore[attr-defined]
        pipeline._api_version = "1.0.0"  # noqa: SLF001  # type: ignore[attr-defined]

        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
            "pref_name": ["Ethanol"],
            "molecule_structures": [
                {"canonical_smiles": "CCO", "standard_inchi_key": "LFQSCWFLJHTTHZ-UHFFFAOYSA-N"},
            ],
            "molecule_properties": [
                {"full_mwt": 46.07, "alogp": 0.31},
            ],
        })

        result = pipeline.transform(df)

        assert "canonical_smiles" in result.columns
        assert "standard_inchi_key" in result.columns
        assert "full_mwt" in result.columns
        assert "alogp" in result.columns
        assert "_chembl_db_version" in result.columns
        assert "_api_version" in result.columns
        assert result["_chembl_db_version"].iloc[0] == "31"  # noqa: SLF001  # type: ignore[attr-defined]
        assert result["_api_version"].iloc[0] == "1.0.0"  # noqa: SLF001  # type: ignore[attr-defined]

    def test_augment_metadata(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test metadata augmentation with ChEMBL versions."""
        pipeline = TestItemChemblPipeline(config=pipeline_config_fixture, run_id=run_id)
        pipeline._chembl_db_version = "31"  # noqa: SLF001  # type: ignore[attr-defined]
        pipeline._api_version = "1.0.0"  # noqa: SLF001  # type: ignore[attr-defined]

        metadata = {"pipeline_version": "0.1.0"}
        df = pd.DataFrame({"molecule_chembl_id": ["CHEMBL1"]})

        result = pipeline.augment_metadata(metadata, df)

        assert result["chembl_db_version"] == "31"
        assert result["api_version"] == "1.0.0"
        assert result["pipeline_version"] == "0.1.0"

    def test_extract_page_items(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test extraction of page items from ChEMBL response."""
        payload = {
            "molecules": [
                {"molecule_chembl_id": "CHEMBL1"},
                {"molecule_chembl_id": "CHEMBL2"},
            ],
            "page_meta": {"limit": 25, "offset": 0},
        }

        result = TestItemChemblPipeline._extract_page_items(payload)  # noqa: SLF001  # type: ignore[attr-defined]

        assert len(result) == 2
        assert result[0]["molecule_chembl_id"] == "CHEMBL1"
        assert result[1]["molecule_chembl_id"] == "CHEMBL2"

    def test_next_link_extraction(self, pipeline_config_fixture: PipelineConfig, run_id: str) -> None:
        """Test extraction of next link from page_meta."""
        base_url = "https://www.ebi.ac.uk/chembl/api/data"

        # Full URL in next link
        payload = {
            "page_meta": {
                "next": "https://www.ebi.ac.uk/chembl/api/data/molecule.json?limit=25&offset=25",
            },
        }
        result = TestItemChemblPipeline._next_link(payload, base_url)  # noqa: SLF001  # type: ignore[attr-defined]
        assert result == "/molecule.json?limit=25&offset=25"

        # Relative path in next link
        payload = {
            "page_meta": {
                "next": "/molecule.json?limit=25&offset=50",
            },
        }
        result = TestItemChemblPipeline._next_link(payload, base_url)  # noqa: SLF001  # type: ignore[attr-defined]
        assert result == "/molecule.json?limit=25&offset=50"

        # No next link
        payload = {"page_meta": {"next": None}}
        result = TestItemChemblPipeline._next_link(payload, base_url)  # noqa: SLF001  # type: ignore[attr-defined]
        assert result is None

