"""External API adapters for document enrichment."""

from bioetl.adapters._normalizer_helpers import get_bibliography_normalizers
from bioetl.adapters.base import ExternalAdapter
from bioetl.adapters.crossref import CrossrefAdapter
from bioetl.adapters.openalex import OpenAlexAdapter
from bioetl.adapters.pubchem import PubChemAdapter
from bioetl.adapters.pubmed import PubMedAdapter
from bioetl.adapters.semantic_scholar import SemanticScholarAdapter

__all__ = [
    "ExternalAdapter",
    "PubMedAdapter",
    "CrossrefAdapter",
    "OpenAlexAdapter",
    "SemanticScholarAdapter",
    "PubChemAdapter",
    "get_bibliography_normalizers",
]

