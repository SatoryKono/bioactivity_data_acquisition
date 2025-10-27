"""HTTP clients for external literature sources."""

from library.clients.exceptions import (
    APIClientError,
    APIConnectionError,
    APIRateLimitError,
    APITimeoutError,
    ResourceNotFoundError,
    SchemaValidationError,
)

from .bioactivity import BioactivityClient
from .chembl import ChEMBLClient
from .crossref import CrossrefClient
from .openalex import OpenAlexClient
from .pubmed import PubMedClient
from .semantic_scholar import SemanticScholarClient
from .session import get_shared_session, reset_shared_session

__all__ = [
    # Exceptions
    "APIClientError",
    "APIConnectionError",
    "APIRateLimitError",
    "APITimeoutError",
    "ResourceNotFoundError",
    "SchemaValidationError",
    # Clients
    "BioactivityClient",
    "ChEMBLClient",
    "CrossrefClient",
    "OpenAlexClient",
    "PubMedClient",
    "SemanticScholarClient",
    "get_shared_session",
    "reset_shared_session",
]
