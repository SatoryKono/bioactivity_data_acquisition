from __future__ import annotations

"""Клиенты-обогащатели по внешним источникам."""

from .base_provider import BaseDataProvider
from .crossref import CrossrefClient
from .openalex import OpenAlexClient
from .pubchem import PubChemClient
from .pubmed import PubmedClient
from .routes import create_route_provider_class
from .semantic_scholar import SemanticScholarClient
from .uniprot import UniProtClient

__all__ = [
    "BaseDataProvider",
    "CrossrefClient",
    "OpenAlexClient",
    "PubChemClient",
    "PubmedClient",
    "SemanticScholarClient",
    "UniProtClient",
    "create_route_provider_class",
]
