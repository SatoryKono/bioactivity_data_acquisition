from __future__ import annotations

from bioetl.clients.enricher_base import BaseEnricherClient, EnricherClientOptions


class _PagingApiClient:
    def __init__(self, payload, pages=None) -> None:  # noqa: ANN001 - тестовая заглушка
        self.payload = payload
        self.pages = pages or []
        self.fetch_one_calls: list[dict] = []
        self.fetch_batch_calls: list[dict] = []

    def fetch_one(  # noqa: ANN001 - тестовая заглушка
        self,
        endpoint: str,
        *,
        params=None,
        headers=None,
        timeout_sec=None,
        max_retries=None,
    ):
        self.fetch_one_calls.append(
            {
                "endpoint": endpoint,
                "params": params,
                "headers": headers,
                "timeout_sec": timeout_sec,
                "max_retries": max_retries,
            }
        )
        return self.payload

    def fetch_batch(  # noqa: ANN001 - тестовая заглушка
        self,
        endpoint: str,
        *,
        params=None,
        headers=None,
        page_key="results",
        next_key="next",
        page_param="page",
        timeout_sec=None,
        max_retries=None,
    ):
        self.fetch_batch_calls.append(
            {
                "endpoint": endpoint,
                "params": params,
                "headers": headers,
                "page_key": page_key,
                "next_key": next_key,
                "page_param": page_param,
                "timeout_sec": timeout_sec,
                "max_retries": max_retries,
            }
        )
        return iter(self.pages)

    def close(self) -> None:
        """Mock close method for testing."""


def test_fetch_one_falls_back_to_payload_when_page_empty():
    payload = {"custom": []}
    api_client = _PagingApiClient(payload)
    client = BaseEnricherClient(api_client, "dummy")

    assert list(client.fetch_one("/path", page_key="custom")) == [
        {"result": payload}
    ]
    assert api_client.fetch_one_calls == [
        {
            "endpoint": "/path",
            "params": None,
            "headers": None,
            "timeout_sec": None,
            "max_retries": None,
        }
    ]


def test_fetch_batch_uses_effective_options_for_pagination():
    pages = [
        {"items": [{"id": 1}]},
        {"items": [{"id": 2}]},
    ]
    options = EnricherClientOptions(
        page_key="items", next_key="cursor", page_param="cursor_param"
    )
    api_client = _PagingApiClient(None, pages=pages)
    client = BaseEnricherClient(api_client, "dummy", options=options)

    assert list(client.fetch_batch("/entities", params={"q": "x"})) == [
        {"id": 1},
        {"id": 2},
    ]
    assert api_client.fetch_batch_calls == [
        {
            "endpoint": "/entities",
            "params": {"q": "x"},
            "headers": None,
            "page_key": "items",
            "next_key": "cursor",
            "page_param": "cursor_param",
            "timeout_sec": None,
            "max_retries": None,
        }
    ]


def test_fetch_batch_allows_per_call_overrides():
    pages = [{"alt": [{"name": "a"}]}]
    options = EnricherClientOptions(
        page_key="items", next_key="cursor", page_param="cursor_param"
    )
    api_client = _PagingApiClient(None, pages=pages)
    client = BaseEnricherClient(api_client, "dummy", options=options)

    assert list(
        client.fetch_batch(
            "/entities",
            page_key="alt",
            next_key="alt_next",
            page_param="alt_page",
        )
    ) == [{"name": "a"}]
    assert api_client.fetch_batch_calls == [
        {
            "endpoint": "/entities",
            "params": None,
            "headers": None,
            "page_key": "alt",
            "next_key": "alt_next",
            "page_param": "alt_page",
            "timeout_sec": None,
            "max_retries": None,
        }
    ]
