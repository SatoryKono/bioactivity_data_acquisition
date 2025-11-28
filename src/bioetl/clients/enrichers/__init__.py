"""Клиенты для внешних источников обогащения.

Алиас ``_BaseEnricherClient`` сохранён для обратной совместимости и выдаёт
``DeprecationWarning`` при обращении; используйте ``BaseEnricherClient`` напрямую.
"""

from __future__ import annotations

import importlib
import sys
import warnings
from typing import Any

from pathlib import Path

from .base import BaseEnricherClient, RouteConfig, RouteEnricherMixin
from bioetl.clients.enrichers.providers import (
    CrossrefClient,
    OpenAlexClient,
    PubChemClient,
    PubmedClient,
    SemanticScholarClient,
    UniProtClient,
)
from bioetl.clients.enrichers.factory import (
    EnricherClientFactory,
    EnricherClientOptions,
    EnricherEntity,
    NULL_ENRICHER_FACTORY,
)
from bioetl.clients.enrichers.facade import (
    ClientMethodStrategy,
    EnricherFacade,
    EnrichmentStrategy,
    build_enricher_facade,
    NullEnricherFacade,
)
from bioetl.clients.enrichers.strategy_registry import StrategyRegistry

_PROVIDERS_PATH = str(Path(__file__).with_name("providers"))
if _PROVIDERS_PATH not in __path__:
    __path__.append(_PROVIDERS_PATH)

_DEPRECATED_MODULES: dict[str, str] = {
    "crossref": "bioetl.clients.enrichers.providers.crossref",
    "openalex": "bioetl.clients.enrichers.providers.openalex",
    "pubchem": "bioetl.clients.enrichers.providers.pubchem",
    "pubmed": "bioetl.clients.enrichers.providers.pubmed",
    "semantic_scholar": "bioetl.clients.enrichers.providers.semantic_scholar",
    "uniprot": "bioetl.clients.enrichers.providers.uniprot",
}

__all__ = [
    "BaseEnricherClient",
    "RouteConfig",
    "RouteEnricherMixin",
    "_BaseEnricherClient",
    "CrossrefClient",
    "EnricherClientFactory",
    "EnricherClientOptions",
    "EnricherEntity",
    "NULL_ENRICHER_FACTORY",
    "StrategyRegistry",
    "OpenAlexClient",
    "PubChemClient",
    "PubmedClient",
    "SemanticScholarClient",
    "UniProtClient",
]


def __getattr__(name: str) -> Any:
    if name == "_BaseEnricherClient":
        warnings.warn(
            "'_BaseEnricherClient' устарел; используйте 'BaseEnricherClient'",
            DeprecationWarning,
            stacklevel=2,
        )
        return BaseEnricherClient
    if name in _DEPRECATED_MODULES:
        new_path = _DEPRECATED_MODULES[name]
        warnings.warn(
            f"'bioetl.clients.enrichers.{name}' перемещён в '{new_path}'",
            DeprecationWarning,
            stacklevel=2,
        )
        module = importlib.import_module(new_path)
        sys.modules[f"{__name__}.{name}"] = module
        return module
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
