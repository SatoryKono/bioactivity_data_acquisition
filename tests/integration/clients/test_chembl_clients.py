from __future__ import annotations

from collections.abc import Iterable
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

clients_pkg = sys.modules.setdefault("bioetl.clients", types.ModuleType("bioetl.clients"))
clients_pkg.__path__ = [str(ROOT / "clients")]

chembl_pkg = sys.modules.setdefault("bioetl.clients.chembl", types.ModuleType("bioetl.clients.chembl"))
chembl_pkg.__path__ = [str(ROOT / "clients" / "chembl")]

pagination_spec = importlib.util.spec_from_file_location(
    "bioetl.core.http.pagination", ROOT / "core" / "http" / "pagination.py"
)
assert pagination_spec and pagination_spec.loader
pagination_module = importlib.util.module_from_spec(pagination_spec)
sys.modules["bioetl.core.http.pagination"] = pagination_module
pagination_spec.loader.exec_module(pagination_module)

api_entity_client_spec = importlib.util.spec_from_file_location(
    "bioetl.core.http.api_entity_client", ROOT / "core" / "http" / "api_entity_client.py"
)
assert api_entity_client_spec and api_entity_client_spec.loader
api_entity_client_module = importlib.util.module_from_spec(api_entity_client_spec)
sys.modules["bioetl.core.http.api_entity_client"] = api_entity_client_module
api_entity_client_spec.loader.exec_module(api_entity_client_module)
api_entity_client_module.BaseApiEntityClient.pagination_strategy = None  # type: ignore[assignment]

adapter_factory_spec = importlib.util.spec_from_file_location(
    "bioetl.clients.chembl.adapter_factory", ROOT / "clients" / "chembl" / "adapter_factory.py"
)
assert adapter_factory_spec and adapter_factory_spec.loader
adapter_factory_module = importlib.util.module_from_spec(adapter_factory_spec)
sys.modules["bioetl.clients.chembl.adapter_factory"] = adapter_factory_module
adapter_factory_spec.loader.exec_module(adapter_factory_module)

base_spec = importlib.util.spec_from_file_location(
    "bioetl.clients.chembl.base", ROOT / "clients" / "chembl" / "base.py"
)
assert base_spec and base_spec.loader
base_module = importlib.util.module_from_spec(base_spec)
sys.modules["bioetl.clients.chembl.base"] = base_module
base_spec.loader.exec_module(base_module)
base_module.BaseChemblAdapterFactory = adapter_factory_module.BaseChemblAdapterFactory
base_module.resolve_pagination_strategy = adapter_factory_module.resolve_pagination_strategy

ChemblEntityClient = base_module.ChemblEntityClient
DefaultPaginationStrategy = pagination_module.DefaultPaginationStrategy


class DummyChemblTransport:
    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self.pages = pages
        self.closed = False
        self.calls: list[dict[str, Any]] = []
        self.pagination_strategy = DefaultPaginationStrategy()

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json: Any | None = None,
    ) -> dict[str, Any]:
        del method, headers, json
        page_num = int((params or {}).get("page", 1))
        self.calls.append({"path": path, "params": dict(params or {}), "page": page_num})
        try:
            return self.pages[page_num - 1]
        except IndexError:
            return {"results": []}

    def close(self) -> None:  # pragma: no cover - trivial flag setter
        self.closed = True


class DummyChemblAdapter:
    def __init__(self, transport: DummyChemblTransport) -> None:
        self.base_transport = transport
        self.pagination_strategy = transport.pagination_strategy
        self.metadata: dict[str, Any] = {}

    def request(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.base_transport.request(*args, **kwargs)

    def close(self) -> None:
        self.base_transport.close()


class DummyAdapterFactory:
    def __init__(self, transport: DummyChemblTransport) -> None:
        self.transport = transport

    def ensure_adapter(self, _: Any) -> DummyChemblAdapter:
        return DummyChemblAdapter(self.transport)


def test_chembl_fetch_many_paginates_with_default_strategy(monkeypatch: pytest.MonkeyPatch) -> None:
    pages = [
        {"page": 1, "results": [{"id": 1}, {"id": 2}]},
        {"page": 2, "results": [{"id": 3}]},
        {"page": 3, "results": []},
    ]
    transport = DummyChemblTransport(pages)

    monkeypatch.setattr(base_module, "BaseChemblAdapterFactory", lambda *_, **__: DummyAdapterFactory(transport))

    client = ChemblEntityClient(transport, "assay")

    records = list(client.fetch_many(page_size=2))

    assert records == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert [call["params"] for call in transport.calls] == [
        {"limit": 2},
        {"limit": 2, "page": 2},
        {"limit": 2, "page": 3},
    ]


def test_chembl_fetch_many_stops_on_empty_first_page(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = DummyChemblTransport([{"page": 1, "results": []}])

    monkeypatch.setattr(base_module, "BaseChemblAdapterFactory", lambda *_, **__: DummyAdapterFactory(transport))

    client = ChemblEntityClient(transport, "target")

    assert list(client.fetch_many(page_size=50)) == []
    assert len(transport.calls) == 1


def test_chembl_client_closes_underlying_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = DummyChemblTransport([{"results": []}])

    monkeypatch.setattr(base_module, "BaseChemblAdapterFactory", lambda *_, **__: DummyAdapterFactory(transport))

    client = ChemblEntityClient(transport, "document")

    client.close()

    assert transport.closed is True
