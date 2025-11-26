from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

import pytest
from structlog.testing import capture_logs

from bioetl.clients import NextLinkPagination, PageParamPagination, PaginationStrategy, RequestException
from bioetl.base_classes import BaseApiClient
from bioetl.clients.common import ApiTransportProtocol, UnifiedEntityClientBase
from bioetl.core.http.client_mixins import ApiClientMixin


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

    def close(self) -> None:  # pragma: no cover - noop for protocol compatibility
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
        del initial_response, transport, page_key, next_key, page_param, normalize
        if logger:
            logger.info("paginate_called", path=endpoint, params=dict(params or {}))
        yield from self.payloads


class _DummyEntityClient(UnifiedEntityClientBase):
    def __init__(self, api_client: _DummyApiClient, payloads: list[Mapping[str, Any]]) -> None:
        self._payloads = payloads
        super().__init__(api_client, "dummy", pagination_strategy=_DummyPagination(payloads))

    def default_pagination_strategy(self, *, strategy_name: str | None = None) -> PaginationStrategy:
        del strategy_name
        return _DummyPagination(self._payloads)


class _DummyBaseApiClient(BaseApiClient):
    def __init__(self) -> None:
        self.calls: list[str] = []

    def get_json(self, endpoint: str, *, params=None, headers=None):  # noqa: ANN001 - тестовая заглушка
        del params, headers
        self.calls.append(endpoint)
        return {"results": [{"endpoint": endpoint}]}

    def paginate_json(self, endpoint: str, *, params=None, headers=None, page_key="results", next_key="next", page_param="page"):
        del params, headers, page_key, next_key, page_param
        yield from [{"endpoint": endpoint}, {"endpoint": f"{endpoint}-page2"}]

    def close(self) -> None:  # pragma: no cover - noop for protocol compatibility
        return None


def test_clients_are_exported() -> None:
    # импорт не должен приводить к ImportError
    assert ApiClientMixin
    assert NextLinkPagination
    assert PageParamPagination


def test_fetch_all_uses_bound_logger_and_pagination() -> None:
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
    payloads: list[Mapping[str, Any]] = []
    client = _DummyEntityClient(_DummyApiClient(payloads), payloads)

    with capture_logs() as logs, pytest.raises(RequestException):
        client._wrap_callable(lambda: (_ for _ in ()).throw(ValueError("boom")))

    assert any(entry.get("event") == "api_call_failed" for entry in logs)


def test_fetch_by_ids_reuses_transport_iteration() -> None:
    payloads: list[Mapping[str, Any]] = []
    api_client = _DummyApiClient(payloads)
    client = _DummyEntityClient(api_client, payloads)

    records = list(client.fetch_by_ids(["123", "456"]))

    assert records == [
        {"endpoint": "/dummy/123", "params": {}},
        {"endpoint": "/dummy/456", "params": {}},
    ]
    assert api_client.calls == [
        ("/dummy/123", None),
        ("/dummy/456", None),
    ]


def test_protocol_compatibility_for_clients() -> None:
    assert isinstance(_DummyApiClient([]), ApiTransportProtocol)
    assert isinstance(_DummyBaseApiClient(), BaseApiClient)
