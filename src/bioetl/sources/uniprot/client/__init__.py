"""HTTP clients specialized for UniProt REST endpoints."""

from bioetl.core.deprecation import warn_legacy_client

from .idmapping_client import UniProtIdMappingClient
from .orthologs_client import UniProtOrthologsClient
from .search_client import UniProtSearchClient

warn_legacy_client(__name__, replacement="bioetl.adapters.uniprot")

# Backwards compatibility alias for legacy import paths
UniProtOrthologClient = UniProtOrthologsClient

__all__ = [
    "UniProtSearchClient",
    "UniProtIdMappingClient",
    "UniProtOrthologsClient",
    "UniProtOrthologClient",
]
