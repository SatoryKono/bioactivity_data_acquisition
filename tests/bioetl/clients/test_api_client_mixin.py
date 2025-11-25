from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest
import structlog
from structlog.testing import capture_logs

from bioetl.clients import client_exceptions
from bioetl.clients.common import ApiClientMixin
from bioetl.clients.chembl._base import BaseChemblClient
from bioetl.clients.entities._base import _BaseEntityClient


class _DummyApiClient(ApiClientMixin):
    def __init__(self) -> None:
        self.api_client = MagicMock()
        self.entity = "dummy"
        self._logger = MagicMock()


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


def test_normalize_payload_extracts_results_key() -> None:
    client = _DummyApiClient()
    payload = {"results": [{"a": 1}, {"a": 2}], "next": None}

    assert list(client._normalize_payload(payload)) == [{"a": 1}, {"a": 2}]


def test_normalize_payload_processes_iterable_of_mappings() -> None:
    client = _DummyApiClient()
    payload = [{"id": 1}, {"id": 2}]

    assert list(client._normalize_payload(payload)) == [{"id": 1}, {"id": 2}]


def test_wrap_callable_converts_errors() -> None:
    client = _DummyApiClient()

    with pytest.raises(client_exceptions.RequestException):
        client._wrap_callable(lambda: (_ for _ in ()).throw(ValueError("boom")))

    client._logger.error.assert_called_once_with("api_call_failed", error="boom")


def test_wrap_callable_preserves_bound_logger_context() -> None:
    class _StructuredClient(ApiClientMixin):
        def __init__(self) -> None:
            self._logger = structlog.get_logger(__name__).bind(entity="molecule")
            self.entity = "molecule"

    with capture_logs() as logs:
        client = _StructuredClient()

        with pytest.raises(client_exceptions.RequestException):
            client._wrap_callable(lambda: (_ for _ in ()).throw(RuntimeError("fail")))

    assert logs[0]["event"] == "api_call_failed"
    assert logs[0]["entity"] == "molecule"
