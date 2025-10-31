"""HTTP clients specialized for UniProt REST endpoints."""

from bioetl.core.deprecation import warn_legacy_client

from .search_client import UniProtSearchClient
from .idmapping_client import UniProtIdMappingClient
from .orthologs_client import UniProtOrthologsClient

warn_legacy_client(__name__, replacement="bioetl.adapters.uniprot")

# Backwards compatibility alias for legacy import paths
UniProtOrthologClient = UniProtOrthologsClient

__all__ = [
    "UniProtSearchClient",
    "UniProtIdMappingClient",
    "UniProtOrthologsClient",
    "UniProtOrthologClient",
]
