import pytest

from pathlib import Path
import importlib.util
import sys
import types

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_ROOT))

def _bootstrap_package(name: str, path: Path) -> None:
    if name in sys.modules:
        return

    spec = importlib.util.spec_from_loader(name, loader=None, is_package=True)
    module = importlib.util.module_from_spec(spec)
    module.__path__ = [str(path)]
    module.__package__ = name
    if module.__spec__:
        module.__spec__.submodule_search_locations = module.__path__
    sys.modules[name] = module


_bootstrap_package("bioetl", SRC_ROOT / "bioetl")
_bootstrap_package("bioetl.clients", SRC_ROOT / "bioetl/clients")
_bootstrap_package("bioetl.clients.chembl", SRC_ROOT / "bioetl/clients/chembl")


def _load_package(name: str, init_path: Path):
    spec = importlib.util.spec_from_file_location(
        name, init_path, submodule_search_locations=[str(init_path.parent)]
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    if spec and spec.loader:
        spec.loader.exec_module(module)
    return module


factories_module = _load_package(
    "bioetl.clients.chembl.factories",
    SRC_ROOT / "bioetl/clients/chembl/factories.py",
)
pagination_module = _load_package(
    "bioetl.clients.chembl.pagination", SRC_ROOT / "bioetl/clients/chembl/pagination.py"
)
interfaces_module = _load_package(
    "bioetl.core.http.interfaces", SRC_ROOT / "bioetl/core/http/interfaces.py"
)

BaseChemblAdapterFactory = factories_module.BaseChemblAdapterFactory
ApiTransportProtocol = interfaces_module.ApiTransportProtocol
DefaultPaginationStrategy = pagination_module.DefaultPaginationStrategy
PaginationStrategy = pagination_module.PaginationStrategy


class DummyTransport(ApiTransportProtocol):
    def __init__(self, response: dict | None = None, strategy: PaginationStrategy | None = None):
        self.response = response or {}
        self.pagination_strategy = strategy

    def request(self, method: str, path: str, **_: object):  # noqa: ARG002
        return self.response

    def close(self) -> None:  # pragma: no cover - noop for tests
        return None


@pytest.mark.parametrize(
    "explicit, strategy_name, transport_strategy, expected",
    [
        (
            DefaultPaginationStrategy(),
            "custom",
            DefaultPaginationStrategy(),
            "explicit",
        ),
        (
            None,
            "custom",
            DefaultPaginationStrategy(),
            "custom",
        ),
        (
            None,
            None,
            DefaultPaginationStrategy(),
            "transport",
        ),
    ],
)
def test_adapter_factory_priority(explicit, strategy_name, transport_strategy, expected):
    custom_strategy = DefaultPaginationStrategy()
    factory = BaseChemblAdapterFactory(
        pagination_strategy=explicit,
        pagination_strategy_name=strategy_name,
        pagination_factories={"custom": lambda: custom_strategy}
        if strategy_name
        else None,
    )
    adapter = factory.ensure_adapter(DummyTransport(strategy=transport_strategy))

    mapping = {
        "explicit": explicit,
        "custom": custom_strategy,
        "transport": transport_strategy,
    }
    assert adapter.pagination_strategy is mapping[expected]


def test_adapter_factory_captures_metadata_from_transport_response():
    response = {"page_meta": {"release": "v1"}}
    factory = BaseChemblAdapterFactory()
    adapter = factory.ensure_adapter(DummyTransport(response=response))

    adapter.request("GET", "/status")

    assert adapter.metadata == {"release": "v1"}
