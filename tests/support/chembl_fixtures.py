"""Фикстуры для тестирования ChEMBL-пайплайнов."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from infrastructure.clients.client_chembl import ChemblClient
from infrastructure.clients.chembl_entity_factory import ChemblClientBundle
from infrastructure.http.api_client import UnifiedAPIClient


@pytest.fixture
def mock_chembl_bundle_with_data(
    mock_chembl_api_client: MagicMock,
    sample_ids: Sequence[str],
    id_column: str,
) -> ChemblClientBundle:
    """Мок bundle с предустановленными данными для тестирования extract_by_ids."""
    bundle = MagicMock(spec=ChemblClientBundle)
    bundle.chembl_client = MagicMock(spec=ChemblClient)
    bundle.chembl_client.handshake.return_value = {"chembl_db_version": "33"}
    bundle.api_client = mock_chembl_api_client
    bundle.entity_client = MagicMock()
    
    # Настраиваем entity_client для возврата данных
    def mock_iterate_by_ids(
        ids: Sequence[str],
        select_fields: Sequence[str] | None = None,
    ) -> Sequence[dict[str, Any]]:
        return [{id_column: id_val, "pref_name": f"Test {id_val}"} for id_val in ids]
    
    bundle.entity_client.iterate_by_ids = mock_iterate_by_ids
    bundle.entity_name = "test_entity"
    bundle.source_name = "chembl"
    bundle.base_url = "https://www.ebi.ac.uk/chembl/api/data"
    bundle.entity_config = None
    bundle.source_config = None
    return bundle


@pytest.fixture
def sample_ids() -> list[str]:
    """Пример валидных ChEMBL ID для тестов."""
    return ["CHEMBL1", "CHEMBL2", "CHEMBL3"]


@pytest.fixture
def id_column() -> str:
    """Имя колонки ID по умолчанию."""
    return "target_chembl_id"

