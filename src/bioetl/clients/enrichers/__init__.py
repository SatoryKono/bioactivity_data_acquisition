"""Клиенты для внешних источников обогащения.

Алиас ``_BaseEnricherClient`` сохранён для обратной совместимости и выдаёт
``DeprecationWarning`` при обращении; используйте ``BaseEnricherClient`` напрямую.
"""

from __future__ import annotations

import warnings
from typing import Any

from .base import BaseEnricherClient, RouteConfig, RouteEnricherMixin
from bioetl.clients.enrichers.crossref import CrossrefClient
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
from bioetl.clients.enrichers.openalex import OpenAlexClient
from bioetl.clients.enrichers.pubchem import PubChemClient
from bioetl.clients.enrichers.pubmed import PubmedClient
from bioetl.clients.enrichers.semantic_scholar import SemanticScholarClient
from bioetl.clients.enrichers.uniprot import UniProtClient

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
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
