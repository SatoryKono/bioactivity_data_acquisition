"""Integration tests for Activity molecule join module."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.clients.chembl import ChemblClient
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.pipelines.activity.join_molecule import join_activity_with_molecule


@pytest.fixture
def mock_chembl_client() -> ChemblClient:
    """Create a mock ChemblClient for testing."""
    mock_api_client = MagicMock(spec=UnifiedAPIClient)
    return ChemblClient(mock_api_client)


@pytest.fixture
def sample_activity_df() -> pd.DataFrame:
    """Sample activity DataFrame for join testing."""
    return pd.DataFrame({
        "activity_id": [1, 2, 3, 4, 5],
        "record_id": [100, 101, 102, 103, None],
        "molecule_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3", "CHEMBL4", "CHEMBL5"],
    })


@pytest.fixture
def molecule_join_config() -> dict[str, Any]:
    """Configuration for molecule join."""
    return {
        "enabled": True,
        "page_limit": 1000,
        "batch_size": 25,
    }


@pytest.mark.integration
class TestActivityMoleculeJoin:
    """Test suite for activity molecule join."""

    def test_join_activity_with_molecule_adds_columns(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        molecule_join_config: dict[str, Any],
    ) -> None:
        """Test that molecule join adds expected columns."""
        # Mock API responses
        mock_chembl_client.paginate = MagicMock(  # type: ignore[method-assign]
            return_value=iter([
                {
                    "activity_id": 1,
                    "record_id": 100,
                    "molecule_chembl_id": "CHEMBL1",
                },
                {
                    "activity_id": 2,
                    "record_id": 101,
                    "molecule_chembl_id": "CHEMBL2",
                },
            ])
        )
        mock_chembl_client.fetch_molecules_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL1": {
                    "molecule_chembl_id": "CHEMBL1",
                    "pref_name": "Aspirin",
                    "molecule_synonyms": [],
                },
                "CHEMBL2": {
                    "molecule_chembl_id": "CHEMBL2",
                    "pref_name": "Ibuprofen",
                    "molecule_synonyms": [],
                },
            }
        )

        # Mock compound_record response
        def mock_compound_record_paginate(endpoint: str, **kwargs: Any) -> Any:
            if endpoint == "/compound_record.json":
                return iter([
                    {
                        "record_id": 100,
                        "compound_key": "KEY1",
                        "compound_name": "Compound1",
                    },
                    {
                        "record_id": 101,
                        "compound_key": "KEY2",
                        "compound_name": "Compound2",
                    },
                ])
            return iter([])

        mock_chembl_client.paginate = MagicMock(side_effect=mock_compound_record_paginate)  # type: ignore[method-assign]

        result = join_activity_with_molecule(
            sample_activity_df,
            mock_chembl_client,
            molecule_join_config,
        )

        # Проверка наличия всех выходных колонок
        expected_columns = ["activity_id", "molecule_key", "molecule_name", "compound_key", "compound_name"]
        assert all(col in result.columns for col in expected_columns)

    def test_join_activity_with_molecule_compound_key(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        molecule_join_config: dict[str, Any],
    ) -> None:
        """Test that compound_key is correctly extracted from compound_record."""
        # Mock compound_record response
        def mock_paginate(endpoint: str, **kwargs: Any) -> Any:
            if endpoint == "/compound_record.json":
                return iter([
                    {
                        "record_id": 100,
                        "compound_key": "TEST_KEY",
                        "compound_name": "Test Compound",
                    },
                ])
            return iter([])

        mock_chembl_client.paginate = MagicMock(side_effect=mock_paginate)  # type: ignore[method-assign]
        mock_chembl_client.fetch_molecules_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL1": {
                    "molecule_chembl_id": "CHEMBL1",
                    "pref_name": "Aspirin",
                    "molecule_synonyms": [],
                },
            }
        )

        result = join_activity_with_molecule(
            sample_activity_df,
            mock_chembl_client,
            molecule_join_config,
        )

        # Проверка compound_key
        assert "compound_key" in result.columns
        # Для record_id=100 должен быть TEST_KEY
        row_with_record_100 = result[result["activity_id"] == 1]
        if not row_with_record_100.empty:
            compound_key = row_with_record_100.iloc[0]["compound_key"]
            # Проверяем, что compound_key не NA и равен TEST_KEY
            assert not pd.isna(compound_key), "compound_key is NA for activity_id=1"
            assert compound_key == "TEST_KEY"

    def test_join_activity_with_molecule_compound_name_pref_name(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        molecule_join_config: dict[str, Any],
    ) -> None:
        """Test that molecule_name uses pref_name when available."""
        mock_chembl_client.paginate = MagicMock(return_value=iter([]))  # type: ignore[method-assign]
        mock_chembl_client.fetch_molecules_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL1": {
                    "molecule_chembl_id": "CHEMBL1",
                    "pref_name": "Aspirin",
                    "molecule_synonyms": [],
                },
            }
        )

        result = join_activity_with_molecule(
            sample_activity_df,
            mock_chembl_client,
            molecule_join_config,
        )

        # Проверка molecule_name
        assert "molecule_name" in result.columns
        # Используем activity_id для фильтрации, так как molecule_chembl_id не сохраняется в результате
        row_with_chembl1 = result[result["activity_id"] == 1]
        if not row_with_chembl1.empty:
            molecule_name = row_with_chembl1.iloc[0]["molecule_name"]
            # Проверяем, что molecule_name не NA и равен Aspirin
            assert not pd.isna(molecule_name), "molecule_name is NA for activity_id=1"
            assert molecule_name == "Aspirin"

    def test_join_activity_with_molecule_compound_name_synonym_fallback(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        molecule_join_config: dict[str, Any],
    ) -> None:
        """Test that molecule_name falls back to first synonym when pref_name is missing."""
        mock_chembl_client.paginate = MagicMock(return_value=iter([]))  # type: ignore[method-assign]
        mock_chembl_client.fetch_molecules_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL2": {
                    "molecule_chembl_id": "CHEMBL2",
                    "pref_name": None,
                    "molecule_synonyms": [
                        {"molecule_synonym": "Synonym1"},
                        {"molecule_synonym": "Synonym2"},
                    ],
                },
            }
        )

        result = join_activity_with_molecule(
            sample_activity_df,
            mock_chembl_client,
            molecule_join_config,
        )

        # Проверка fallback на первый синоним
        # Используем activity_id для фильтрации, так как molecule_chembl_id не сохраняется в результате
        row_with_chembl2 = result[result["activity_id"] == 2]
        if not row_with_chembl2.empty:
            molecule_name = row_with_chembl2.iloc[0]["molecule_name"]
            # Проверяем, что molecule_name не NA и равен Synonym1
            assert not pd.isna(molecule_name), "molecule_name is NA for activity_id=2"
            assert molecule_name == "Synonym1"

    def test_join_activity_with_molecule_empty_dataframe(
        self,
        mock_chembl_client: ChemblClient,
        molecule_join_config: dict[str, Any],
    ) -> None:
        """Test handling of empty DataFrame."""
        empty_df = pd.DataFrame()

        result = join_activity_with_molecule(
            empty_df,
            mock_chembl_client,
            molecule_join_config,
        )

        # Должен вернуть пустой DataFrame с правильными колонками
        expected_columns = ["activity_id", "molecule_key", "molecule_name", "compound_key", "compound_name"]
        assert all(col in result.columns for col in expected_columns)
        assert result.empty

    def test_join_activity_with_molecule_missing_molecule_chembl_id(
        self,
        mock_chembl_client: ChemblClient,
        molecule_join_config: dict[str, Any],
    ) -> None:
        """Test handling of missing molecule_chembl_id column."""
        df_missing_col = pd.DataFrame({
            "activity_id": [1, 2],
            "record_id": [100, 101],
            # molecule_chembl_id отсутствует
        })

        result = join_activity_with_molecule(
            df_missing_col,
            mock_chembl_client,
            molecule_join_config,
        )

        # Должен вернуть пустой DataFrame с правильными колонками
        expected_columns = ["activity_id", "molecule_key", "molecule_name", "compound_key", "compound_name"]
        assert all(col in result.columns for col in expected_columns)

    def test_join_activity_with_molecule_missing_molecules(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        molecule_join_config: dict[str, Any],
    ) -> None:
        """Test handling of missing molecules in API response."""
        mock_chembl_client.paginate = MagicMock(return_value=iter([]))  # type: ignore[method-assign]
        mock_chembl_client.fetch_molecules_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={}  # Пустой ответ
        )

        result = join_activity_with_molecule(
            sample_activity_df,
            mock_chembl_client,
            molecule_join_config,
        )

        # Проверка, что molecule_name заполнен из molecule_key (fallback)
        assert "molecule_name" in result.columns
        assert "molecule_key" in result.columns
        # molecule_name должен быть равен molecule_key, если molecule не найден
        for _, row in result.iterrows():
            if pd.notna(row.get("molecule_key")):
                assert row["molecule_name"] == row["molecule_key"]

    def test_join_activity_with_molecule_output_columns(
        self,
        mock_chembl_client: ChemblClient,
        sample_activity_df: pd.DataFrame,
        molecule_join_config: dict[str, Any],
    ) -> None:
        """Test that output contains only required columns."""
        mock_chembl_client.paginate = MagicMock(return_value=iter([]))  # type: ignore[method-assign]
        mock_chembl_client.fetch_molecules_by_ids = MagicMock(  # type: ignore[method-assign]
            return_value={
                "CHEMBL1": {
                    "molecule_chembl_id": "CHEMBL1",
                    "pref_name": "Aspirin",
                    "molecule_synonyms": [],
                },
            }
        )

        result = join_activity_with_molecule(
            sample_activity_df,
            mock_chembl_client,
            molecule_join_config,
        )

        # Проверка, что колонки строго соответствуют требованиям
        expected_columns = ["activity_id", "molecule_key", "molecule_name", "compound_key", "compound_name"]
        assert list(result.columns) == expected_columns

