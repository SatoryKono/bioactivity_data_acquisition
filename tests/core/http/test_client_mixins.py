# pylint: disable=protected-access
# pyright: reportPrivateUsage=false
# ruff: noqa: SLF001
"""Tests for ApiClientMixin and ClosableMixin."""
from typing import cast
from unittest.mock import ANY, MagicMock, call

import pytest
import structlog
from structlog.testing import capture_logs

from bioetl.clients import exceptions as client_exceptions
from bioetl.clients.chembl import BaseChemblClient, ChemblEntityClient
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.clients.enrichers._base import _BaseEnricherClient
from bioetl.core.http.api_entity_client import (
    BaseApiEntityClient as _BaseEntityClient
)
from bioetl.core.http.client_mixins import ApiClientMixin, ClosableMixin


class _DummyApiClient(ApiClientMixin, ClosableMixin):
    """Dummy client for testing mixins."""

    def __init__(self) -> None:
        self.transport = MagicMock(spec=ApiTransportProtocol)
        self.entity = "dummy"
        self._logger = MagicMock()


def test_entity_client_fetch_by_ids_normalizes_and_logs_payloads() -> None:
    """Test that fetch_by_ids normalizes payloads and logs calls."""
    transport = MagicMock(spec=ApiTransportProtocol)
    transport.request.side_effect = [
        {"id": "10", "value": 1},
        [
            {"id": "11", "value": 2},
            {"id": "12", "value": 3},
        ],
    ]

    client = _BaseEntityClient(
        transport=transport,
        pagination=MagicMock(),
        entity="targets",
    )
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
    """Test that ChemblEntityClient uses the shared fetch_by_ids logic."""
    transport = MagicMock(spec=ApiTransportProtocol)
    transport.request.side_effect = [
        {"chembl_id": "CHEMBL1"},
        {"chembl_id": "CHEMBL2"},
    ]

    client = ChemblEntityClient(transport=transport, entity="molecule")
    client._logger = MagicMock()

    result = list(client.fetch_by_ids(["1", "2"]))

    assert result == [
        {"chembl_id": "CHEMBL1"},
        {"chembl_id": "CHEMBL2"},
    ]
    assert transport.request.call_args_list == [
        call("GET", "/molecule/1", headers=None, params=None, json=None),
        call("GET", "/molecule/2", headers=None, params=None, json=None),
    ]
    assert client._logger.info.call_args_list == [
        call("api_call", entity="molecule", entity_id="1"),
        call("api_call", entity="molecule", entity_id="2"),
    ]


def test_entity_client_fetch_all_propagates_pagination_arguments() -> None:
    """Test that fetch_all propagates arguments to pagination strategy."""
    transport = MagicMock(spec=ApiTransportProtocol)
    transport.request.return_value = {"items": [{"id": 1}]}
    pagination = MagicMock()
    pagination.iter_pages.return_value = iter(
        [{"items": [{"id": 1}, {"id": 2}]}]
    )

    client = _BaseEntityClient(
        transport=transport,
        pagination=pagination,
        entity="targets",
    )
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
        client.transport,
        endpoint="/targets",
        params={"limit": 2, "foo": "bar"},
        logger=client._logger,
        page_key="items",
        next_key="next_link",
        page_param=None,
        normalize=ANY,
    )
    transport.request.assert_called_once_with(
        "GET",
        "/targets",
        params={"limit": 2, "foo": "bar"},
    )


def test_chembl_client_fetch_all_propagates_pagination_arguments() -> None:
    """Test that ChemblEntityClient propagates pagination arguments."""
    transport = MagicMock(spec=ApiTransportProtocol)
    transport.request.return_value = {"items": [{"id": 1}]}
    pagination = MagicMock()
    pagination.iter_pages.return_value = iter([{"items": [{"id": 1}]}])

    client = ChemblEntityClient(
        transport=transport,
        entity="molecule",
        pagination_strategy=pagination,
    )
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
        ANY,
        endpoint="/molecule",
        params={"limit": 10, "offset": 5},
        logger=client._logger,
        page_key="items",
        next_key="next_link",
        page_param="page_num",
        normalize=ANY,
    )
    transport.request.assert_called_once_with(
        "GET",
        "/molecule",
        params={"limit": 10, "offset": 5},
        headers=None,
        json=None,
    )


def test_entity_client_fetch_all_wraps_errors() -> None:
    """Test that fetch_all wraps runtime errors in RequestException."""
    transport = MagicMock(spec=ApiTransportProtocol)
    pagination = MagicMock()
    pagination.iter_pages.side_effect = RuntimeError("boom")
    transport.request.return_value = {}

    client = _BaseEntityClient(
        transport=transport,
        pagination=pagination,
        entity="targets",
    )
    client._logger = MagicMock()

    with pytest.raises(client_exceptions.RequestException):
        list(client.fetch_all())


def test_chembl_client_fetch_all_wraps_errors() -> None:
    """Test that ChemblEntityClient wraps errors in fetch_all."""
    transport = MagicMock(spec=ApiTransportProtocol)
    pagination = MagicMock()
    pagination.iter_pages.side_effect = RuntimeError("boom")
    transport.request.return_value = {}

    client = ChemblEntityClient(
        transport=transport,
        entity="molecule",
        pagination_strategy=pagination,
    )
    client._logger = MagicMock()

    with pytest.raises(client_exceptions.RequestException):
        list(client.fetch_all())


def test_normalize_payload_extracts_results_key() -> None:
    """Test payload normalization with a results key."""
    client = _DummyApiClient()
    payload = {"results": [{"a": 1}, {"a": 2}], "next": None}

    assert list(client._normalize_payload(payload)) == [{"a": 1}, {"a": 2}]


def test_normalize_payload_processes_iterable_of_mappings() -> None:
    """Test payload normalization with a list of mappings."""
    client = _DummyApiClient()
    payload = [{"id": 1}, {"id": 2}]

    assert list(client._normalize_payload(payload)) == [{"id": 1}, {"id": 2}]


def test_wrap_callable_converts_errors() -> None:
    """Test that _wrap_callable converts exceptions and logs them."""
    client = _DummyApiClient()

    with pytest.raises(client_exceptions.RequestException):
        client._wrap_callable(
            lambda: (_ for _ in ()).throw(ValueError("boom"))
        )

    mock_error = cast(MagicMock, client._logger.error)
    mock_error.assert_called_once_with(
        "api_call_failed",
        error="boom",
    )


def test_wrap_callable_preserves_bound_logger_context() -> None:
    """Test that _wrap_callable preserves the logger context."""
    class _StructuredClient(ApiClientMixin, ClosableMixin):
        """Structured client with bound logger."""

        def __init__(self) -> None:
            self.transport = MagicMock(spec=ApiTransportProtocol)
            self._logger = structlog.get_logger(__name__).bind(
                entity="molecule"
            )
            self.entity = "molecule"

    with capture_logs() as logs:
        client = _StructuredClient()

        with pytest.raises(client_exceptions.RequestException):
            client._wrap_callable(
                lambda: (_ for _ in ()).throw(RuntimeError("fail"))
            )

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
    """Test that mixins are not duplicated in the MRO."""
    mro = cls.mro()

    assert issubclass(cls, ApiClientMixin)
    assert issubclass(cls, ClosableMixin)
    assert mro.count(ApiClientMixin) == 1
    assert mro.count(ClosableMixin) == 1
