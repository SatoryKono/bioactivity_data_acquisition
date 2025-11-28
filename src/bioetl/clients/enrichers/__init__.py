"""Клиенты для внешних источников обогащения."""

from .base import BaseEnricherClient
from bioetl.clients.enrichers.crossref import CrossrefClient
from bioetl.clients.enrichers.factory import (
    EnricherClientFactory,
    EnricherClientOptions,
)
from bioetl.clients.enrichers.openalex import OpenAlexClient
from bioetl.clients.enrichers.pubchem import PubChemClient
from bioetl.clients.enrichers.pubmed import PubmedClient
from bioetl.clients.enrichers.semantic_scholar import SemanticScholarClient
from bioetl.clients.enrichers.uniprot import UniProtClient

__all__ = [
    "BaseEnricherClient",
    "CrossrefClient",
    "EnricherClientFactory",
    "EnricherClientOptions",
    "OpenAlexClient",
    "PubChemClient",
    "PubmedClient",
    "SemanticScholarClient",
    "UniProtClient",
]
