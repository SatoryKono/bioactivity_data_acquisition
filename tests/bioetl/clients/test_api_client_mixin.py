from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest

from bioetl.clients import client_exceptions
from bioetl.clients.chembl._base import BaseChemblClient
from bioetl.clients.entities._base import _BaseEntityClient


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


def test_entity_client_fetch_all_propagates_pagination_arguments() -> None:
    api_client = MagicMock()
    pagination = MagicMock()
    pagination.paginate.return_value = iter([{"id": 1}, {"id": 2}])

    client = _BaseEntityClient(api_client=api_client, entity="targets", pagination_strategy=pagination)
    client._logger = MagicMock()

    result = list(
        client.fetch_all(
            page_size=2,
            params={"foo": "bar"},
            page_key="items",
            next_key="next_link",
            page_param=None,
        )
    )

    assert result == [{"id": 1}, {"id": 2}]
    pagination.paginate.assert_called_once_with(
        api_client,
        "/targets",
        params={"limit": 2, "foo": "bar"},
        logger=client._logger,
        page_key="items",
        next_key="next_link",
        page_param=None,
    )


def test_chembl_client_fetch_all_propagates_pagination_arguments() -> None:
    api_client = MagicMock()
    pagination = MagicMock()
    pagination.paginate.return_value = iter([{"id": 1}])

    client = BaseChemblClient(api_client=api_client, entity="molecule", pagination_strategy=pagination)
    client._logger = MagicMock()

    result = list(
        client.fetch_all(
            page_size=10,
            params={"offset": 5},
            page_key="items",
            next_key="next_link",
            page_param="page_num",
        )
    )

    assert result == [{"id": 1}]
    pagination.paginate.assert_called_once_with(
        api_client,
        "/molecule",
        params={"limit": 10, "offset": 5},
        logger=client._logger,
        page_key="items",
        next_key="next_link",
        page_param="page_num",
    )


def test_entity_client_fetch_all_wraps_errors() -> None:
    api_client = MagicMock()
    pagination = MagicMock()
    pagination.paginate.side_effect = RuntimeError("boom")

    client = _BaseEntityClient(api_client=api_client, entity="targets", pagination_strategy=pagination)
    client._logger = MagicMock()

    with pytest.raises(client_exceptions.RequestException):
        list(client.fetch_all())


def test_chembl_client_fetch_all_wraps_errors() -> None:
    api_client = MagicMock()
    pagination = MagicMock()
    pagination.paginate.side_effect = RuntimeError("boom")

    client = BaseChemblClient(api_client=api_client, entity="molecule", pagination_strategy=pagination)
    client._logger = MagicMock()

    with pytest.raises(client_exceptions.RequestException):
        list(client.fetch_all())
