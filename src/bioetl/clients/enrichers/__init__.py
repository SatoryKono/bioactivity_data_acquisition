"""Клиенты для внешних источников обогащения.

Приватный алиас ``_BaseEnricherClient`` удалён; используйте ``BaseEnricherClient``
напрямую.
"""

from .base import BaseEnricherClient, RouteConfig, RouteEnricherMixin
from bioetl.clients.enrichers.crossref import CrossrefClient
from bioetl.clients.enrichers.factory import (
    EnricherClientFactory,
    EnricherClientOptions,
)
from bioetl.clients.enrichers.facade import (
    ClientMethodStrategy,
    EnricherFacade,
    EnrichmentStrategy,
    NULL_ENRICHER_FACTORY,
    build_enricher_facade,
    NullEnricherFacade,
)
from bioetl.clients.enrichers.openalex import OpenAlexClient
from bioetl.clients.enrichers.pubchem import PubChemClient
from bioetl.clients.enrichers.pubmed import PubmedClient
from bioetl.clients.enrichers.semantic_scholar import SemanticScholarClient
from bioetl.clients.enrichers.uniprot import UniProtClient

__all__ = [
    "BaseEnricherClient",
    "RouteConfig",
    "RouteEnricherMixin",
    "CrossrefClient",
    "EnricherClientFactory",
    "EnricherClientOptions",
    "EnricherFacade",
    "EnrichmentStrategy",
    "ClientMethodStrategy",
    "build_enricher_facade",
    "NULL_ENRICHER_FACTORY",
    "NullEnricherFacade",
    "OpenAlexClient",
    "PubChemClient",
    "PubmedClient",
    "SemanticScholarClient",
    "UniProtClient",
]
