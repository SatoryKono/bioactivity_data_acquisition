from __future__ import annotations

import pytest
from unittest.mock import MagicMock, call

from bioetl.clients.chembl._base import BaseChemblClient
from bioetl.clients.entities._base import _BaseEntityClient
from bioetl.clients import client_exceptions


def test_entity_client_fetch_by_ids_normalizes_and_logs_payloads() -> None:
    api_client = MagicMock()
    api_client.get_json.side_effect = [
        {"id": "10", "value": 1},
        [
            {"id": "11", "value": 2},
            {"id": "12", "value": 3},
        ],
    ]

    client = _BaseEntityClient(api_client=api_client, entity="targets")
    client._logger = MagicMock()

    result = list(client.fetch_by_ids(["10", "11"]))

    assert result == [
        {"id": "10", "value": 1},
        {"id": "11", "value": 2},
        {"id": "12", "value": 3},
    ]
    assert api_client.get_json.call_args_list == [
        call("/targets/10"),
        call("/targets/11"),
    ]
    assert client._logger.info.call_args_list == [
        call("api_call", entity="targets", entity_id="10"),
        call("api_call", entity="targets", entity_id="11"),
    ]


def test_chembl_client_fetch_by_ids_uses_shared_iterator() -> None:
    api_client = MagicMock()
    api_client.get_json.side_effect = [
        {"chembl_id": "CHEMBL1"},
        {"chembl_id": "CHEMBL2"},
    ]

    client = BaseChemblClient(api_client=api_client, entity="molecule")
    client._logger = MagicMock()

    result = list(client.fetch_by_ids([1, 2]))

    assert result == [
        {"chembl_id": "CHEMBL1"},
        {"chembl_id": "CHEMBL2"},
    ]
    assert api_client.get_json.call_args_list == [
        call("/molecule/1"),
        call("/molecule/2"),
    ]
    assert client._logger.info.call_args_list == [
        call("api_call", entity="molecule", entity_id="1"),
        call("api_call", entity="molecule", entity_id="2"),
    ]


def test_entity_client_fetch_all_uses_consistent_pagination_defaults() -> None:
    api_client = MagicMock()
    api_client.paginate_json.return_value = iter(
        [
            {"results": [{"id": 1}, {"id": 2}]},
            {"results": [{"id": 3}]},
        ]
    )

    client = _BaseEntityClient(api_client=api_client, entity="targets")

    assert list(client.fetch_all(page_size=2)) == [{"id": 1}, {"id": 2}, {"id": 3}]
    api_client.paginate_json.assert_called_once_with(
        "/targets",
        params={"limit": 2},
        page_key="results",
        next_key="next",
        page_param=None,
    )


def test_chembl_client_fetch_all_uses_shared_signature() -> None:
    api_client = MagicMock()
    api_client.paginate_json.return_value = iter(
        [
            {"results": [{"chembl_id": "CHEMBL1"}]},
            {"results": [{"chembl_id": "CHEMBL2"}]},
        ]
    )

    client = BaseChemblClient(api_client=api_client, entity="molecule")

    assert list(client.fetch_all(page_size=2, params={"limit": 99})) == [
        {"chembl_id": "CHEMBL1"},
        {"chembl_id": "CHEMBL2"},
    ]
    api_client.paginate_json.assert_called_once_with(
        "/molecule",
        params={"limit": 2},
        page_key="results",
        next_key="next",
        page_param=None,
    )


@pytest.mark.parametrize("client_factory", [BaseChemblClient, _BaseEntityClient])
def test_fetch_all_wraps_unexpected_errors(client_factory) -> None:
    api_client = MagicMock()
    api_client.paginate_json.side_effect = ValueError("boom")

    client = client_factory(api_client=api_client, entity="targets")
    client._logger = MagicMock()

    with pytest.raises(client_exceptions.RequestException):
        list(client.fetch_all())
    api_client.paginate_json.assert_called_once()
    client._logger.error.assert_called_once()
