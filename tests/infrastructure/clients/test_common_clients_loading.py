"""Test loading and initialization of common API clients."""

from collections.abc import Iterator, Mapping
from typing import Any

import pytest
from structlog.testing import capture_logs

from bioetl.clients import RequestException
from bioetl.core.http import (
    ApiClientMixin,
    NextLinkPagination,
    PageParamPagination,
    PaginationStrategy,
)
from bioetl.core.http.api_entity_client import BaseApiEntityClient
from bioetl.core.http.interfaces import ApiTransportProtocol, BaseApiClient


class _DummyApiClient(ApiTransportProtocol):
    def __init__(self, payloads: list[Mapping[str, Any]]) -> None:
        self.payloads = payloads
        self.calls: list[tuple[str, Mapping[str, Any] | None]] = []

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
    ) -> Mapping[str, Any]:
        del headers, json
        self.calls.append((path, params))
        return {"results": [{"endpoint": path, "params": dict(params or {})}]}

    def close(
        self,
    ) -> None:  # pragma: no cover - noop for protocol compatibility
        return None


class _DummyPagination(PaginationStrategy):
    def __init__(self, payloads: list[Mapping[str, Any]]) -> None:
        self.payloads = payloads

    def iter_pages(
        self,
        initial_response: Mapping[str, Any],
        transport: Any,
        *,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        logger: Any | None = None,
        page_key: str | None = None,
        next_key: str | None = None,
        page_param: str | None = None,
        normalize: Any | None = None,
    ) -> Iterator[Any]:
        del (
            initial_response,
            transport,
            page_key,
            next_key,
            page_param,
            normalize,
        )
        if logger:
            logger.info(
                "paginate_called",
                path=endpoint,
                params=dict(params or {}),
            )
        yield from self.payloads


class _DummyEntityClient(BaseApiEntityClient):
    def __init__(
        self,
        api_client: _DummyApiClient,
        payloads: list[Mapping[str, Any]],
    ) -> None:
        self._payloads = payloads
        pagination = _DummyPagination(payloads)
        api_client.pagination_strategy = pagination
        super().__init__(
            api_client,
            pagination,
            entity="dummy",
        )


class _DummyBaseApiClient(BaseApiClient):
    def __init__(self) -> None:
        self.calls: list[str] = []

    def get_json(
        self, endpoint: str, *, params=None, headers=None
    ) -> Mapping[str, Any]:  # noqa: ANN001 - тестовая заглушка
        del params, headers
        self.calls.append(endpoint)
        return {"results": [{"endpoint": endpoint}]}

    def paginate_json(
        self,
        endpoint: str,
        *,
        params=None,
        headers=None,
        page_key="results",
        next_key="next",
        page_param="page",
    ) -> Iterator[Mapping[str, Any]]:
        del params, headers, page_key, next_key, page_param
        yield from [{"endpoint": endpoint}, {"endpoint": f"{endpoint}-page2"}]

    def close(
        self,
    ) -> None:  # pragma: no cover - noop for protocol compatibility
        return None


def test_clients_are_exported() -> None:
    """Test that all expected client classes are properly exported."""
    # импорт не должен приводить к ImportError
    assert ApiClientMixin
    assert NextLinkPagination
    assert PageParamPagination


def test_fetch_all_uses_bound_logger_and_pagination() -> None:
    """Test that fetch_all uses logger and pagination correctly."""
    payloads = [
        {"results": [{"id": 1}, {"id": 2}]},
        {"results": [{"id": 3}]},
    ]
    api_client = _DummyApiClient(payloads)
    client = _DummyEntityClient(api_client, payloads)

    with capture_logs() as logs:
        records = list(client.fetch_all(page_size=2))

    assert records == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert any(entry.get("event") == "paginate_called" for entry in logs)
    assert all(entry.get("entity") == "dummy" for entry in logs)


def test_wrap_callable_converts_exceptions_and_logs_error() -> None:
    """Test that _wrap_callable converts exceptions and logs errors."""
    payloads: list[Mapping[str, Any]] = []
    client = _DummyEntityClient(_DummyApiClient(payloads), payloads)

    with capture_logs() as logs, pytest.raises(RequestException):
        client._wrap_callable(  # noqa: SLF001 - testing protected method
            lambda: (_ for _ in ()).throw(ValueError("boom"))
        )

    assert any(entry.get("event") == "api_call_failed" for entry in logs)


def test_fetch_batch_reuses_transport_iteration() -> None:
    """Test that fetch_batch reuses transport iteration correctly."""
    payloads: list[Mapping[str, Any]] = []
    api_client = _DummyApiClient(payloads)
    client = _DummyEntityClient(api_client, payloads)

    records = list(client.fetch_batch(["123", "456"]))

    assert records == [
        {"endpoint": "/dummy/123", "params": {}},
        {"endpoint": "/dummy/456", "params": {}},
    ]
    assert api_client.calls == [
        ("/dummy/123", None),
        ("/dummy/456", None),
    ]


def test_protocol_compatibility_for_clients() -> None:
    """Test that dummy clients implement required protocols correctly."""
    assert isinstance(_DummyApiClient([]), ApiTransportProtocol)
    assert isinstance(_DummyBaseApiClient(), BaseApiClient)
