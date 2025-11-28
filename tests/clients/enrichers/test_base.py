from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3] / "src" / "bioetl"

bioetl_pkg = sys.modules.setdefault("bioetl", types.ModuleType("bioetl"))
bioetl_pkg.__path__ = [str(ROOT)]

clients_pkg = sys.modules.setdefault("bioetl.clients", types.ModuleType("bioetl.clients"))
clients_pkg.__path__ = [str(ROOT / "clients")]

enrichers_pkg = sys.modules.setdefault(
    "bioetl.clients.enrichers", types.ModuleType("bioetl.clients.enrichers")
)
enrichers_pkg.__path__ = [str(ROOT / "clients" / "enrichers")]

spec = importlib.util.spec_from_file_location(
    "bioetl.clients.enrichers.base", ROOT / "clients" / "enrichers" / "base.py"
)
assert spec and spec.loader  # защита от странных путей в окружении
base_module = importlib.util.module_from_spec(spec)
sys.modules["bioetl.clients.enrichers.base"] = base_module
spec.loader.exec_module(base_module)

BaseEnricherClient = base_module.BaseEnricherClient
EnricherClientOptions = base_module.EnricherClientOptions


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
