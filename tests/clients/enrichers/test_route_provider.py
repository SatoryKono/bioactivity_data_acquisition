import importlib.util
import sys
import types
from pathlib import Path
from typing import Any

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def _prepare_namespaces() -> None:
    bioetl_pkg = sys.modules.setdefault("bioetl", types.ModuleType("bioetl"))
    bioetl_pkg.__path__ = [str(SRC_ROOT / "bioetl")]

    clients_pkg = sys.modules.setdefault(
        "bioetl.clients", types.ModuleType("bioetl.clients")
    )
    clients_pkg.__path__ = [str(SRC_ROOT / "bioetl" / "clients")]

    enrichers_pkg = sys.modules.setdefault(
        "bioetl.clients.enrichers", types.ModuleType("bioetl.clients.enrichers")
    )
    enrichers_pkg.__path__ = [str(SRC_ROOT / "bioetl" / "clients" / "enrichers")]

    providers_pkg = sys.modules.setdefault(
        "bioetl.clients.enrichers.providers",
        types.ModuleType("bioetl.clients.enrichers.providers"),
    )
    providers_pkg.__path__ = [
        str(SRC_ROOT / "bioetl" / "clients" / "enrichers" / "providers")
    ]


def _load_module(module_name: str, path: Path) -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module {module_name} from {path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_prepare_namespaces()
_load_module(
    "bioetl.clients.enrichers.base",
    SRC_ROOT / "bioetl" / "clients" / "enrichers" / "base.py",
)

crossref = _load_module(
    "bioetl.clients.enrichers.providers.crossref",
    SRC_ROOT / "bioetl" / "clients" / "enrichers" / "providers" / "crossref.py",
)
pubchem = _load_module(
    "bioetl.clients.enrichers.providers.pubchem",
    SRC_ROOT / "bioetl" / "clients" / "enrichers" / "providers" / "pubchem.py",
)

CrossrefClient = crossref.CrossrefClient
PubChemClient = pubchem.PubChemClient


class DummyApiClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def fetch_one(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> dict[str, Any]:
        self.calls.append((
            "fetch_one", path, params or {}, headers,
            timeout_sec, max_retries
        ))
        return {"results": [{"path": path, "params": params}]}

    def fetch_batch(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        page_key: str | None = None,
        next_key: str | None = None,
        page_param: str | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append(
            (
                "fetch_batch",
                path,
                params or {},
                headers,
                page_key,
                next_key,
                page_param,
                timeout_sec,
                max_retries,
            )
        )
        return [
            {
                "results": [
                    {
                        "path": path,
                        "params": params,
                        "page_key": page_key,
                        "next_key": next_key,
                        "page_param": page_param,
                    }
                ]
            }
        ]


@pytest.mark.parametrize(
    "client_cls, fetch_value, search_value, expected_fetch_path, expected_search_path, query_param",
    [
        (
            CrossrefClient,
            "10.1000/xyz",
            "test query",
            "/works/10.1000/xyz",
            "/works",
            "query",
        ),
        (
            PubChemClient,
            "2244",
            "C1=CC=CC=C1",
            "/compound/2244",
            "/compound/search",
            "smiles",
        ),
    ],
)
def test_route_provider_builds_paths_and_params(
    client_cls,
    fetch_value,
    search_value,
    expected_fetch_path,
    expected_search_path,
    query_param,
):
    transport = DummyApiClient()
    client = client_cls(transport)

    list(client.fetch_one(fetch_value, params={"foo": "bar"}))
    list(client.fetch_batch(search_value, params={"page": "2"}))

    fetch_call = transport.calls[0]
    search_call = transport.calls[1]

    assert fetch_call[1] == expected_fetch_path
    assert fetch_call[2] == {"foo": "bar"}
    assert fetch_call[3] is None  # headers

    assert search_call[1] == expected_search_path
    assert search_call[2] == {query_param: search_value, "page": "2"}
    assert search_call[3] is None  # headers
    assert search_call[4] == "results"


@pytest.mark.parametrize(
    "client_cls, alias, target",
    [
        (CrossrefClient, "fetch", "fetch_one"),
        (CrossrefClient, "search", "fetch_batch"),
        (PubChemClient, "fetch_by_cid", "fetch_one"),
        (PubChemClient, "search_by_smiles", "fetch_batch"),
    ],
)
def test_deprecated_aliases_emit_warning(client_cls, alias, target):
    transport = DummyApiClient()
    client = client_cls(transport)
    method = getattr(client, alias)

    with pytest.warns(DeprecationWarning) as captured:
        list(method("value"))

    assert target in str(captured[0].message)
    assert transport.calls, "Underlying method should be invoked"
