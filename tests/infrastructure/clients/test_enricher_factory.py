from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from bioetl.clients.enrichers.factory import (
    EnricherApiConfig,
    EnricherClientFactory,
    EnricherClientOptions,
)
from bioetl.core.http.pagination import DefaultPaginationStrategy


class _RecordingApiClient:
    """Test double that records API calls without inheriting from BaseApiClient."""

    def __init__(self, options: EnricherClientOptions) -> None:
        self.options = options
        self.calls: list[tuple[str, Mapping[str, Any] | None]] = []
        self.closed = False

    def get_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Mapping[str, Any] | list[Mapping[str, Any]]:
        del headers, timeout_sec, max_retries
        self.calls.append((endpoint, params))
        return {"results": [{"endpoint": endpoint, "params": params}]}

        def paginate_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Sequence[Mapping[str, Any]]:
        del endpoint, params, headers, page_key, next_key, page_param
        del timeout_sec, max_retries
        return []

    def fetch_one(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Any:
        del endpoint, params, headers, timeout_sec, max_retries
        return None

    def fetch_batch(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        page_key: str = "results",
        next_key: str | None = "next",
        page_param: str | None = "page",
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Any:
        del headers, page_key, next_key, page_param, timeout_sec, max_retries
        return [{"endpoint": endpoint, "params": params}]

    def iterate_records(
        self,
        *,
        ids: Sequence[str] | None = None,
        page_size: int | None = None,
        fetcher: Any | None = None,
    ) -> Any:
        del ids, page_size, fetcher
        return iter(())

    def close(self) -> None:
        self.closed = True

    # Required abstract attributes (duck-typed)
    @property
    def default_timeout_sec(self) -> float:
        return 30.0

    @property
    def default_max_retries(self) -> int:
        return 3

    @property
    def pagination_strategy(self) -> None:
        return None


def test_enricher_factory_wraps_api_client_with_options() -> None:
    """Test that factory correctly applies timeout and retry options."""

    def builder(options: EnricherClientOptions) -> _RecordingApiClient:
        return _RecordingApiClient(options)

    factory = EnricherClientFactory(builder).with_options(
        timeout_sec=2.5, max_retries=2
    )
    client = factory.crossref()

    records = list(client.fetch("10.1000/example"))

    assert records == [{"endpoint": "/works/10.1000/example", "params": None}]
    assert client.api_client.timeout_sec == 2.5
    assert client.api_client.max_retries == 2
    assert isinstance(client.api_client._api_client, _RecordingApiClient)
    assert client.api_client._api_client.options.timeout_sec == 2.5


def test_enricher_factory_from_config_accepts_api_client_instance() -> None:
    """Test that factory can accept a pre-configured API client instance."""
    api_client = _RecordingApiClient(EnricherClientOptions())

    factory = EnricherClientFactory.from_config(
        {"api_client": api_client, "options": {"timeout_sec": 1.0, "max_retries": 1}}
    )

    assert isinstance(factory, EnricherClientFactory)
    client = factory.pubchem()
    assert client.api_client.timeout_sec == 1.0


def test_enricher_factory_preserves_api_pagination_config() -> None:
    """Test that pagination config from API settings is preserved."""
    captured: dict[str, Any] = {}

    def builder(
        options: EnricherClientOptions, api_config: Any | None = None
    ) -> _RecordingApiClient:
        captured["api_config"] = api_config
        return _RecordingApiClient(options)

    pagination = DefaultPaginationStrategy(page_key="items")

    factory = EnricherClientFactory.from_config(
        {"api_client": builder, "api": {"pagination": pagination}}
    )

    assert isinstance(factory, EnricherClientFactory)
    _ = factory.crossref()

    assert isinstance(captured["api_config"], EnricherApiConfig)
    assert captured["api_config"].pagination is pagination


def test_enricher_factory_ignores_top_level_api_fields_without_warnings() -> None:
    """Test that top-level API fields are ignored without warnings."""
    captured: dict[str, Any] = {}

    def builder(
        options: EnricherClientOptions, api_config: Any | None = None
    ) -> _RecordingApiClient:
        captured["options"] = options
        captured["api_config"] = api_config
        return _RecordingApiClient(options)

    factory = EnricherClientFactory.from_config(
        {
            "api_client": builder,
            "options": {"timeout_sec": 3.0, "max_retries": 5},
            "timeout_sec": 10.0,
            "retries": 2,
            "pagination": DefaultPaginationStrategy(),
        }
    )

    assert isinstance(factory, EnricherClientFactory)
    _ = factory.crossref()

    assert captured["options"].timeout_sec == 3.0
    assert captured["options"].max_retries == 5
    # Верхнеуровневые поля игнорируются и не попадают в api_config
    assert isinstance(captured["api_config"], EnricherApiConfig)
    assert captured["api_config"].pagination is None
