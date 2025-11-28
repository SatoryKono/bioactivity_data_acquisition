from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from bioetl.clients.enrichers.factory import (
    EnricherClientFactory,
    EnricherClientOptions,
)
from bioetl.core.http.interfaces import BaseApiClient


class _RecordingApiClient(BaseApiClient):
    def __init__(self, options: EnricherClientOptions) -> None:
        self.options = options
        self.calls: list[tuple[str, Mapping[str, Any] | None]] = []

    def get_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Mapping[str, Any] | list[Mapping[str, Any]]:
        del headers
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
    ) -> Sequence[Mapping[str, Any]]:
        del endpoint, params, headers, page_key, next_key, page_param
        return []

    def iterate_records(
        self,
        *,
        ids: Sequence[str] | None = None,
        page_size: int | None = None,
        fetcher: Any | None = None,
    ):
        del ids, page_size, fetcher
        return iter(())

    def close(self) -> None:  # pragma: no cover - noop
        return None


def test_enricher_factory_wraps_api_client_with_options():
    def builder(options: EnricherClientOptions) -> _RecordingApiClient:
        return _RecordingApiClient(options)

    factory = EnricherClientFactory(builder).with_options(timeout_sec=2.5, max_retries=2)
    client = factory.crossref()

    records = list(client.fetch("10.1000/example"))

    assert records == [{"endpoint": "/works/10.1000/example", "params": None}]
    assert client.api_client.timeout_sec == 2.5
    assert client.api_client.max_retries == 2
    assert isinstance(client.api_client._api_client, _RecordingApiClient)
    assert client.api_client._api_client.options.timeout_sec == 2.5


def test_enricher_factory_from_config_accepts_api_client_instance():
    api_client = _RecordingApiClient(EnricherClientOptions())

    factory = EnricherClientFactory.from_config(
        {"api_client": api_client, "options": {"timeout_sec": 1.0, "max_retries": 1}}
    )

    assert isinstance(factory, EnricherClientFactory)
    client = factory.pubchem()
    assert client.api_client.timeout_sec == 1.0
