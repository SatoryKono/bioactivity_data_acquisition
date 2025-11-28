from __future__ import annotations

"""Клиенты-обогащатели по внешним источникам."""

import importlib
import sys
import warnings
from typing import Any

from .crossref import CrossrefClient
from .openalex import OpenAlexClient
from .pubchem import PubChemClient
from .pubmed import PubmedClient
from .semantic_scholar import SemanticScholarClient
from .uniprot import UniProtClient

__all__ = [
    "CrossrefClient",
    "OpenAlexClient",
    "PubChemClient",
    "PubmedClient",
    "SemanticScholarClient",
    "UniProtClient",
]

_DEPRECATED_ALIASES: dict[str, str] = {
    "crossref": "bioetl.clients.enrichers.crossref",
    "openalex": "bioetl.clients.enrichers.openalex",
    "pubchem": "bioetl.clients.enrichers.pubchem",
    "pubmed": "bioetl.clients.enrichers.pubmed",
    "semantic_scholar": "bioetl.clients.enrichers.semantic_scholar",
    "uniprot": "bioetl.clients.enrichers.uniprot",
}


def __getattr__(name: str) -> Any:
    if name in _DEPRECATED_ALIASES:
        alias = _DEPRECATED_ALIASES[name]
        target = f"bioetl.clients.enrichers.providers.{name}"
        warnings.warn(
            f"Модуль '{alias}' перемещён в '{target}'", DeprecationWarning, stacklevel=2
        )
        module = importlib.import_module(target)
        sys.modules[alias] = module
        return module
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
