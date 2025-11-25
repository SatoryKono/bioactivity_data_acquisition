from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

import pytest
from structlog.testing import capture_logs

from bioetl.clients import (
    ApiClientMixin,
    NextLinkPagination,
    PageParamPagination,
    PaginationStrategy,
    RequestException,
)
from bioetl.clients.common import UnifiedEntityClientBase
from bioetl.infra import PaginationRegistry


class _DummyApiClient:
    def __init__(self, payloads: list[Mapping[str, Any]]) -> None:
        self.payloads = payloads

    def get_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Mapping[str, Any]:
        del headers
        return {"results": [{"endpoint": endpoint, "params": dict(params or {})}]}

    def paginate_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[Mapping[str, Any]]:
        del headers, page_key, next_key, page_param, endpoint, params
        yield from self.payloads

    def close(self) -> None:  # pragma: no cover - noop for protocol compatibility
        return None


class _DummyPagination(PaginationStrategy):
    def __init__(self, payloads: list[Mapping[str, Any]]) -> None:
        self.payloads = payloads

    def paginate(
        self,
        api_client: Any,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        logger: Any | None = None,
        page_key: str | None = None,
        next_key: str | None = None,
        page_param: str | None = None,
        normalize=None,
    ) -> Iterator[Any]:
        del api_client, page_key, next_key, page_param, normalize
        if logger:
            logger.info("paginate_called", path=endpoint, params=dict(params or {}))
        yield from self.payloads


class _DummyEntityClient(UnifiedEntityClientBase):
    def __init__(
        self,
        api_client: _DummyApiClient,
        payloads: list[Mapping[str, Any]],
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_registry: PaginationRegistry | None = None,
    ) -> None:
        self._payloads = payloads
        strategy = pagination_strategy
        if strategy is None and pagination_strategy_name is None:
            strategy = _DummyPagination(payloads)
        super().__init__(
            api_client,
            "dummy",
            pagination_strategy=strategy,
            pagination_strategy_name=pagination_strategy_name,
            pagination_registry=pagination_registry,
        )

    def default_pagination_strategy(
        self,
        *,
        pagination_strategy_name: str | None = None,
        pagination_registry: PaginationRegistry | None = None,
    ) -> PaginationStrategy:
        if pagination_strategy_name and pagination_registry:
            return pagination_registry.get(pagination_strategy_name)
        return _DummyPagination(self._payloads)


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


def test_client_uses_registry_strategy_by_name() -> None:
    registry_payloads = [{"results": [{"id": "registry"}]}]
    registry = PaginationRegistry()
    registry.register("custom", lambda: _DummyPagination(registry_payloads))

    client = _DummyEntityClient(
        _DummyApiClient(registry_payloads),
        [],
        pagination_strategy_name="custom",
        pagination_registry=registry,
    )

    records = list(client.fetch_all())

    assert client.pagination_strategy is not None
    assert isinstance(client.pagination_strategy, _DummyPagination)
    assert records == [{"id": "registry"}]
