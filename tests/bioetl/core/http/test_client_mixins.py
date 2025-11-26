from unittest.mock import MagicMock, call

import pytest
import structlog
from structlog.testing import capture_logs

from bioetl.clients import client_exceptions
from bioetl.clients.chembl._base import BaseChemblClient, ChemblEntityClient
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.clients.enrichers._base import _BaseEnricherClient
from bioetl.clients.entities._base import _BaseEntityClient
from bioetl.core.http import ApiClientMixin, ClosableMixin


class _DummyApiClient(ApiClientMixin, ClosableMixin):
    def __init__(self) -> None:
        self.transport = MagicMock(spec=ApiTransportProtocol)
        self.entity = "dummy"
        self._logger = MagicMock()


def test_entity_client_fetch_by_ids_normalizes_and_logs_payloads() -> None:
    transport = MagicMock(spec=ApiTransportProtocol)
    transport.request.side_effect = [
        {"id": "10", "value": 1},
        [
            {"id": "11", "value": 2},
            {"id": "12", "value": 3},
        ],
    ]

    client = _BaseEntityClient(transport=transport, entity="targets")
    client._logger = MagicMock()

    result = list(client.fetch_by_ids(["10", "11"]))

    assert result == [
        {"id": "10", "value": 1},
        {"id": "11", "value": 2},
        {"id": "12", "value": 3},
    ]
    assert transport.request.call_args_list == [
        call("GET", "/targets/10"),
        call("GET", "/targets/11"),
    ]
    assert client._logger.info.call_args_list == [
        call("api_call", entity="targets", entity_id="10"),
        call("api_call", entity="targets", entity_id="11"),
    ]


def test_chembl_client_fetch_by_ids_uses_shared_iterator() -> None:
    transport = MagicMock(spec=ApiTransportProtocol)
    transport.request.side_effect = [
        {"chembl_id": "CHEMBL1"},
        {"chembl_id": "CHEMBL2"},
    ]

    client = ChemblEntityClient(transport=transport, entity="molecule")
    client._logger = MagicMock()

    result = list(client.fetch_by_ids([1, 2]))

    assert result == [
        {"chembl_id": "CHEMBL1"},
        {"chembl_id": "CHEMBL2"},
    ]
    assert transport.request.call_args_list == [
        call("GET", "/molecule/1"),
        call("GET", "/molecule/2"),
    ]
    assert client._logger.info.call_args_list == [
        call("api_call", entity="molecule", entity_id="1"),
        call("api_call", entity="molecule", entity_id="2"),
    ]


def test_entity_client_fetch_all_propagates_pagination_arguments() -> None:
    transport = MagicMock(spec=ApiTransportProtocol)
    transport.request.return_value = {"items": [{"id": 1}]}
    pagination = MagicMock()
    pagination.iter_pages.return_value = iter([{"items": [{"id": 1}, {"id": 2}]}])

    client = _BaseEntityClient(transport=transport, entity="targets", pagination_strategy=pagination)
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
    pagination.iter_pages.assert_called_once_with(
        {"items": [{"id": 1}]},
        transport,
        endpoint="/targets",
        params={"limit": 2, "foo": "bar"},
        logger=client._logger,
        page_key="items",
        next_key="next_link",
        page_param=None,
        normalize=client._normalize_payload,
    )
    transport.request.assert_called_once_with("GET", "/targets", params={"limit": 2, "foo": "bar"})


def test_chembl_client_fetch_all_propagates_pagination_arguments() -> None:
    transport = MagicMock(spec=ApiTransportProtocol)
    transport.request.return_value = {"items": [{"id": 1}]}
    pagination = MagicMock()
    pagination.iter_pages.return_value = iter([{"items": [{"id": 1}]}])

    client = ChemblEntityClient(transport=transport, entity="molecule", pagination_strategy=pagination)
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
    pagination.iter_pages.assert_called_once_with(
        {"items": [{"id": 1}]},
        transport,
        endpoint="/molecule",
        params={"limit": 10, "offset": 5},
        logger=client._logger,
        page_key="items",
        next_key="next_link",
        page_param="page_num",
        normalize=client._normalize_payload,
    )
    transport.request.assert_called_once_with("GET", "/molecule", params={"limit": 10, "offset": 5})


def test_entity_client_fetch_all_wraps_errors() -> None:
    transport = MagicMock(spec=ApiTransportProtocol)
    pagination = MagicMock()
    pagination.iter_pages.side_effect = RuntimeError("boom")
    transport.request.return_value = {}

    client = _BaseEntityClient(transport=transport, entity="targets", pagination_strategy=pagination)
    client._logger = MagicMock()

    with pytest.raises(client_exceptions.RequestException):
        list(client.fetch_all())


def test_chembl_client_fetch_all_wraps_errors() -> None:
    transport = MagicMock(spec=ApiTransportProtocol)
    pagination = MagicMock()
    pagination.iter_pages.side_effect = RuntimeError("boom")
    transport.request.return_value = {}

    client = ChemblEntityClient(transport=transport, entity="molecule", pagination_strategy=pagination)
    client._logger = MagicMock()

    with pytest.raises(client_exceptions.RequestException):
        list(client.fetch_all())


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
    class _StructuredClient(ApiClientMixin, ClosableMixin):
        def __init__(self) -> None:
            self.transport = MagicMock(spec=ApiTransportProtocol)
            self._logger = structlog.get_logger(__name__).bind(entity="molecule")
            self.entity = "molecule"

    with capture_logs() as logs:
        client = _StructuredClient()

        with pytest.raises(client_exceptions.RequestException):
            client._wrap_callable(lambda: (_ for _ in ()).throw(RuntimeError("fail")))

    assert logs[0]["event"] == "api_call_failed"
    assert logs[0]["entity"] == "molecule"


@pytest.mark.parametrize(
    "cls",
    [
        _BaseEntityClient,
        ChemblEntityClient,
        BaseChemblClient,
        _BaseEnricherClient,
    ],
)
def test_mixins_are_not_duplicated_in_mro(cls: type) -> None:
    mro = cls.mro()

    assert issubclass(cls, ApiClientMixin)
    assert issubclass(cls, ClosableMixin)
    assert mro.count(ApiClientMixin) == 1
    assert mro.count(ClosableMixin) == 1
