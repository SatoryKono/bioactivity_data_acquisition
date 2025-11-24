from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from bioetl.base_classes import BaseApiClient
from infrastructure.config.models.http import HTTPClientConfig
from infrastructure.http.api_client import UnifiedAPIClient

pytestmark = pytest.mark.unit


def test_unified_api_client_satisfies_base_contract() -> None:
    client = UnifiedAPIClient(config=HTTPClientConfig())

    assert isinstance(client, BaseApiClient)


def test_batch_get_delegates_to_get(monkeypatch: pytest.MonkeyPatch) -> None:
    client = UnifiedAPIClient(config=HTTPClientConfig())
    responses = [{"id": idx} for idx in range(3)]
    get_mock = MagicMock(side_effect=responses)
    monkeypatch.setattr(client, "get", get_mock)

    result = list(client.batch_get(["/1", "/2", "/3"], batch_size=2))

    assert result == responses
    assert get_mock.call_count == 3
    get_mock.assert_any_call("/1", params=None, headers=None)
    get_mock.assert_any_call("/2", params=None, headers=None)
    get_mock.assert_any_call("/3", params=None, headers=None)


def test_search_default_not_implemented() -> None:
    client = UnifiedAPIClient(config=HTTPClientConfig())

    with pytest.raises(NotImplementedError):
        client.search("/search")
