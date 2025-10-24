"""HTTP clients for external literature sources."""
from .base import RateLimitConfig, RateLimiter
from .bioactivity import BioactivityClient
from .chembl import ChEMBLClient, ChEMBLDocumentClient
from .crossref import CrossrefClient
from .exceptions import ApiClientError, RateLimitError
from .openalex import OpenAlexClient
from .pubmed import PubMedClient
from .semantic_scholar import SemanticScholarClient
from .session import get_shared_session, reset_shared_session

__all__ = [
    "ApiClientError",
    "RateLimitConfig",
    "RateLimitError",
    "RateLimiter",
    "BioactivityClient",
    "ChEMBLClient",
    "ChEMBLDocumentClient",
    "CrossrefClient",
    "OpenAlexClient",
    "PubMedClient",
    "SemanticScholarClient",
    "get_shared_session",
    "reset_shared_session",
]
