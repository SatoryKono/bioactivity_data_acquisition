from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any
import importlib.util
import sys
import types

import pytest

ROOT = Path(__file__).resolve().parents[3] / "src" / "bioetl"

bioetl_pkg = sys.modules.setdefault("bioetl", types.ModuleType("bioetl"))
bioetl_pkg.__path__ = [str(ROOT)]

core_pkg = sys.modules.setdefault("bioetl.core", types.ModuleType("bioetl.core"))
core_pkg.__path__ = [str(ROOT / "core")]

http_pkg = sys.modules.setdefault("bioetl.core.http", types.ModuleType("bioetl.core.http"))
http_pkg.__path__ = [str(ROOT / "core" / "http")]

http_init_spec = importlib.util.spec_from_file_location(
    "bioetl.core.http", ROOT / "core" / "http" / "__init__.py"
)
assert http_init_spec and http_init_spec.loader
http_module = importlib.util.module_from_spec(http_init_spec)
sys.modules["bioetl.core.http"] = http_module
http_init_spec.loader.exec_module(http_module)

clients_pkg = sys.modules.setdefault("bioetl.clients", types.ModuleType("bioetl.clients"))
clients_pkg.__path__ = [str(ROOT / "clients")]

enrichers_pkg = sys.modules.setdefault(
    "bioetl.clients.enrichers", types.ModuleType("bioetl.clients.enrichers")
)
enrichers_pkg.__path__ = [str(ROOT / "clients" / "enrichers")]

providers_pkg = sys.modules.setdefault(
    "bioetl.clients.enrichers.providers",
    types.ModuleType("bioetl.clients.enrichers.providers"),
)
providers_pkg.__path__ = [str(ROOT / "clients" / "enrichers" / "providers")]

exceptions_spec = importlib.util.spec_from_file_location(
    "bioetl.clients.exceptions", ROOT / "clients" / "exceptions.py"
)
assert exceptions_spec and exceptions_spec.loader
exceptions_module = importlib.util.module_from_spec(exceptions_spec)
sys.modules["bioetl.clients.exceptions"] = exceptions_module
exceptions_spec.loader.exec_module(exceptions_module)

base_spec = importlib.util.spec_from_file_location(
    "bioetl.clients.enrichers.base", ROOT / "clients" / "enrichers" / "base.py"
)
assert base_spec and base_spec.loader
base_module = importlib.util.module_from_spec(base_spec)
sys.modules["bioetl.clients.enrichers.base"] = base_module
base_spec.loader.exec_module(base_module)

crossref_spec = importlib.util.spec_from_file_location(
    "bioetl.clients.enrichers.providers.crossref",
    ROOT / "clients" / "enrichers" / "providers" / "crossref.py",
)
assert crossref_spec and crossref_spec.loader
crossref_module = importlib.util.module_from_spec(crossref_spec)
sys.modules["bioetl.clients.enrichers.providers.crossref"] = crossref_module
crossref_spec.loader.exec_module(crossref_module)

CrossrefClient = crossref_module.CrossrefClient
exceptions = exceptions_module


class FakePagingApiClient:
    def __init__(
        self,
        *,
        fetch_one_payload: Any = None,
        batch_pages: Iterable[Any] | None = None,
    ) -> None:
        self.fetch_one_payload = fetch_one_payload
        self.batch_pages = list(batch_pages or [])
        self.fetch_one_calls: list[dict[str, Any]] = []
        self.fetch_batch_calls: list[dict[str, Any]] = []
        self.closed = False

    def fetch_one(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Any:
        self.fetch_one_calls.append(
            {
                "endpoint": endpoint,
                "params": params,
                "headers": headers,
                "timeout_sec": timeout_sec,
                "max_retries": max_retries,
            }
        )
        return self.fetch_one_payload

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
    ) -> Iterable[Any]:
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
        return iter(self.batch_pages)

    def close(self) -> None:  # pragma: no cover - simple flag setter
        self.closed = True


class FailingBatchApiClient(FakePagingApiClient):
    def fetch_batch(self, *args: Any, **kwargs: Any) -> Iterable[Any]:  # noqa: ANN001 - test stub
        def _generator() -> Iterable[Any]:
            yield {"results": [{"id": 1}]}
            raise RuntimeError("boom")

        return _generator()


@pytest.mark.parametrize(
    "payload, page_key",
    [({"custom": []}, "custom"), ({"results": []}, None)],
)
def test_route_provider_fetch_one_yields_fallback_when_page_empty(
    payload: Mapping[str, Any], page_key: str | None
) -> None:
    api_client = FakePagingApiClient(fetch_one_payload=payload)
    client = CrossrefClient(api_client)

    result = list(client.fetch_one("10.1000/xyz", page_key=page_key))

    assert result == [{"result": payload}]
    assert api_client.fetch_one_calls == [
        {
            "endpoint": "/works/10.1000/xyz",
            "params": None,
            "headers": None,
            "timeout_sec": None,
            "max_retries": None,
        }
    ]


def test_route_provider_fetch_batch_paginates_and_passes_params() -> None:
    pages = [
        {"results": [{"id": 1}]},
        {"results": [{"id": 2}]},
    ]
    api_client = FakePagingApiClient(batch_pages=pages)
    client = CrossrefClient(api_client)

    records = list(
        client.fetch_batch("aspirin", params={"filter": "type:journal"})
    )

    assert records == [{"id": 1}, {"id": 2}]
    assert api_client.fetch_batch_calls == [
        {
            "endpoint": "/works",
            "params": {"query": "aspirin", "filter": "type:journal"},
            "headers": None,
            "page_key": "results",
            "next_key": "next",
            "page_param": "page",
            "timeout_sec": None,
            "max_retries": None,
        }
    ]


def test_route_provider_closes_transport_on_iteration_error() -> None:
    api_client = FailingBatchApiClient()
    client = CrossrefClient(api_client)

    with pytest.raises(exceptions.RequestException):
        list(client.fetch_batch("broken"))

    assert api_client.closed is True
