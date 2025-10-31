"""UniProt pipeline exports."""

from .client import (
    UniProtIdMappingClient,
    UniProtOrthologsClient,
    UniProtSearchClient,
)
from .merge.service import UniProtEnrichmentResult, UniProtService

__all__ = [
    "UniProtService",
    "UniProtEnrichmentResult",
    "UniProtSearchClient",
    "UniProtIdMappingClient",
    "UniProtOrthologsClient",
]
