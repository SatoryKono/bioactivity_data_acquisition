"""UniProt data access helpers and services."""

from .client import (
    UniProtIdMappingClient,
    UniProtOrthologsClient,
    UniProtSearchClient,
)
from .service import UniProtEnrichmentResult, UniProtService

__all__ = [
    "UniProtService",
    "UniProtEnrichmentResult",
    "UniProtSearchClient",
    "UniProtIdMappingClient",
    "UniProtOrthologsClient",
]
