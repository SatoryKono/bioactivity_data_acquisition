"""Тесты для реестра конфигураций ChEMBL сущностей."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Type
from unittest.mock import MagicMock

import pytest

from bioetl.clients.activity.chembl_activity import ChemblActivityClient
from bioetl.clients.assay.chembl_assay import ChemblAssayClient
from bioetl.clients.chembl_base import ChemblClientProtocol
from bioetl.clients.chembl_entity_client import ChemblEntityClientBase
from bioetl.clients.document.chembl_document import ChemblDocumentClient
from bioetl.clients.target.chembl_target import ChemblTargetClient
from bioetl.clients.testitem.chembl_testitem import ChemblTestitemClient


@pytest.fixture
def mock_chembl_client() -> MagicMock:
    """Заглушка ChemblClientProtocol."""

    return MagicMock(spec=ChemblClientProtocol)


@dataclass(frozen=True)
class _ExpectedConfig:
    endpoint: str
    filter_param: str
    id_key: str
    items_key: str
    log_prefix: str
    base_endpoint_length: int


@pytest.mark.unit
@pytest.mark.parametrize(
    ("client_cls", "expected"),
    [
        (
            ChemblActivityClient,
            _ExpectedConfig(
                endpoint="/activity.json",
                filter_param="activity_id__in",
                id_key="activity_id",
                items_key="activities",
                log_prefix="activity",
                base_endpoint_length=len("/activity.json?"),
            ),
        ),
        (
            ChemblDocumentClient,
            _ExpectedConfig(
                endpoint="/document.json",
                filter_param="document_chembl_id__in",
                id_key="document_chembl_id",
                items_key="documents",
                log_prefix="document",
                base_endpoint_length=len("/document.json?"),
            ),
        ),
        (
            ChemblTargetClient,
            _ExpectedConfig(
                endpoint="/target.json",
                filter_param="target_chembl_id__in",
                id_key="target_chembl_id",
                items_key="targets",
                log_prefix="target",
                base_endpoint_length=len("/target.json?"),
            ),
        ),
        (
            ChemblTestitemClient,
            _ExpectedConfig(
                endpoint="/molecule.json",
                filter_param="molecule_chembl_id__in",
                id_key="molecule_chembl_id",
                items_key="molecules",
                log_prefix="molecule",
                base_endpoint_length=len("/molecule.json?"),
            ),
        ),
        (
            ChemblAssayClient,
            _ExpectedConfig(
                endpoint="/assay.json",
                filter_param="assay_chembl_id__in",
                id_key="assay_chembl_id",
                items_key="assays",
                log_prefix="assay",
                base_endpoint_length=len("/assay.json?"),
            ),
        ),
    ],
)
def test_entity_configs_from_registry(
    mock_chembl_client: MagicMock,
    client_cls: Type[ChemblEntityClientBase],
    expected: _ExpectedConfig,
) -> None:
    """Проверить корректность конфигураций из реестра."""

    client = client_cls(mock_chembl_client)
    config = client._config  # noqa: SLF001

    assert config.endpoint == expected.endpoint
    assert config.filter_param == expected.filter_param
    assert config.id_key == expected.id_key
    assert config.items_key == expected.items_key
    assert config.log_prefix == expected.log_prefix
    assert config.chunk_size == 100
    assert config.supports_list_result is False
    assert config.base_endpoint_length == expected.base_endpoint_length


@pytest.mark.unit
@pytest.mark.parametrize(
    "client_cls",
    [
        ChemblActivityClient,
        ChemblDocumentClient,
        ChemblTargetClient,
        ChemblTestitemClient,
    ],
)
def test_non_assay_clients_keep_url_length_disabled(
    mock_chembl_client: MagicMock,
    client_cls: Type[ChemblEntityClientBase],
) -> None:
    """Другие клиенты не включают проверку длины URL."""

    client = client_cls(mock_chembl_client, max_url_length=2000)
    config = client._config  # noqa: SLF001

    assert config.enable_url_length_check is False


@pytest.mark.unit
def test_assay_client_enables_url_check_with_limit(
    mock_chembl_client: MagicMock,
) -> None:
    """Клиент assay включает проверку длины URL, если задан лимит."""

    client = ChemblAssayClient(mock_chembl_client, max_url_length=2000)
    config = client._config  # noqa: SLF001

    assert config.enable_url_length_check is True


