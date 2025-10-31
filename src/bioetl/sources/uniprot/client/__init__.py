"""HTTP clients specialized for UniProt REST endpoints."""

from .idmapping_client import UniProtIdMappingClient
from .orthologs_client import UniProtOrthologsClient
from .search_client import UniProtSearchClient

__all__ = [
    "UniProtSearchClient",
    "UniProtIdMappingClient",
    "UniProtOrthologsClient",
]
