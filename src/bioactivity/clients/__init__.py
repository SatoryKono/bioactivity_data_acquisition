"""HTTP clients for external literature sources."""
from .base import ApiClientError, BaseApiClient, RateLimitConfig, RateLimiter, RateLimitError
from .bioactivity import BioactivityClient
from .chembl import ChEMBLClient
from .crossref import CrossrefClient
from .openalex import OpenAlexClient
from .pubmed import PubMedClient
from .semantic_scholar import SemanticScholarClient
from .session import get_shared_session, reset_shared_session

__all__ = [
    "ApiClientError",
    "BaseApiClient",
    "RateLimitConfig",
    "RateLimitError",
    "RateLimiter",
    "BioactivityClient",
    "ChEMBLClient",
    "CrossrefClient",
    "OpenAlexClient",
    "PubMedClient",
    "SemanticScholarClient",
    "get_shared_session",
    "reset_shared_session",
]
