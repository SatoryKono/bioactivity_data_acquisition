"""HTTP clients specialized for UniProt REST endpoints."""

from .idmapping_client import UniProtIdMappingClient
from .orthologs_client import UniProtOrthologsClient
from .search_client import UniProtSearchClient

# Backwards compatibility alias for legacy import paths
UniProtOrthologClient = UniProtOrthologsClient

__all__ = [
    "UniProtSearchClient",
    "UniProtIdMappingClient",
    "UniProtOrthologsClient",
    "UniProtOrthologClient",
]
