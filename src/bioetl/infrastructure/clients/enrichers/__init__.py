"""Клиенты для внешних источников обогащения."""

from bioetl.infrastructure.clients.enrichers.crossref import CrossrefClient
from bioetl.infrastructure.clients.enrichers.openalex import OpenAlexClient
from bioetl.infrastructure.clients.enrichers.pubchem import PubChemClient
from bioetl.infrastructure.clients.enrichers.pubmed import PubmedClient
from bioetl.infrastructure.clients.enrichers.semantic_scholar import SemanticScholarClient
from bioetl.infrastructure.clients.enrichers.uniprot import UniProtClient

__all__ = [
    "CrossrefClient",
    "OpenAlexClient",
    "PubChemClient",
    "PubmedClient",
    "SemanticScholarClient",
    "UniProtClient",
]
