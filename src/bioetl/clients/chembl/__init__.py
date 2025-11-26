"""Клиенты ChEMBL entities."""

from importlib import import_module
from typing import Any

__all__ = [
    "ChemblActivityClient",
    "ChemblAssayClient",
    "ChemblDocumentClient",
    "ChemblTargetClient",
    "ChemblTestItemClient",
]

_CLIENT_MODULES = {
    "ChemblActivityClient": "bioetl.clients.chembl.client_activity",
    "ChemblAssayClient": "bioetl.clients.chembl.client_assay",
    "ChemblDocumentClient": "bioetl.clients.chembl.client_document",
    "ChemblTargetClient": "bioetl.clients.chembl.client_target",
    "ChemblTestItemClient": "bioetl.clients.chembl.client_testitem",
}


def __getattr__(name: str) -> Any:
    if name in _CLIENT_MODULES:
        module = import_module(_CLIENT_MODULES[name])
        return getattr(module, name)
    raise AttributeError(name)


def __dir__() -> list[str]:  # pragma: no cover - поддержка автодополнения
    return sorted(__all__ + list(globals().keys()))
