"""HTTP-клиенты BioETL."""

from bioetl.clients.client_exceptions import ConnectionError, HTTPError, RequestException, Timeout
from bioetl.clients.common import (
    ApiTransportProtocol,
    EntityClientProtocol,
    NextLinkPagination,
    PageParamPagination,
    PaginationStrategy,
    cache_entity_client,
)
from bioetl.clients.chembl import (
    ChemblActivityClient,
    ChemblAssayClient,
    ChemblDocumentClient,
    ChemblTargetClient,
    ChemblTestItemClient,
)
from bioetl.core.http import ApiClientMixin
from bioetl.clients.transports import AioHttpTransport, RequestsTransport
from bioetl.clients.factories import default_chembl_factory

__all__ = [
    "ConnectionError",
    "HTTPError",
    "RequestException",
    "Timeout",
    "ApiClientMixin",
    "ApiTransportProtocol",
    "EntityClientProtocol",
    "cache_entity_client",
    "PaginationStrategy",
    "NextLinkPagination",
    "PageParamPagination",
    "ChemblActivityClient",
    "ChemblAssayClient",
    "ChemblDocumentClient",
    "ChemblTargetClient",
    "ChemblTestItemClient",
    "default_chembl_factory",
    "RequestsTransport",
    "AioHttpTransport",
]
