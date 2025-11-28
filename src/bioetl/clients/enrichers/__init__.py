"""Клиенты для внешних источников обогащения.

Модуль помечен кандидатом на удаление после проверки отсутствия
динамических загрузок; оставить под наблюдением до подтверждения.
"""
import sys

from . import base
from .base import BaseEnricherClient
from bioetl.clients.enrichers.crossref import CrossrefClient
from bioetl.clients.enrichers.openalex import OpenAlexClient
from bioetl.clients.enrichers.pubchem import PubChemClient
from bioetl.clients.enrichers.pubmed import PubmedClient
from bioetl.clients.enrichers.semantic_scholar import SemanticScholarClient
from bioetl.clients.enrichers.uniprot import UniProtClient

_BaseEnricherClient = BaseEnricherClient
sys.modules[__name__ + "._base"] = base

__all__ = [
    "BaseEnricherClient",
    "_BaseEnricherClient",
    "CrossrefClient",
    "OpenAlexClient",
    "PubChemClient",
    "PubmedClient",
    "SemanticScholarClient",
    "UniProtClient",
]
