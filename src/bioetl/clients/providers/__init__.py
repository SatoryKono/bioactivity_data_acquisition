from __future__ import annotations

"""Клиенты-обогащатели по внешним источникам."""

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
