"""BioETL Clients and Integrations."""

from bioetl.clients.exceptions import (
    ConnectionError,
    HTTPError,
    RequestException,
    Timeout,
)
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
    ChemblEntityClientFactory,
)
from bioetl.clients.factories import (
    make_chembl_client,
    default_chembl_factory,
)
from bioetl.clients.transports import (
    AioHttpTransport,
    RequestsTransport,
)

__all__ = [
    "ConnectionError",
    "HTTPError",
    "RequestException",
    "Timeout",
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
    "ChemblEntityClientFactory",
    "make_chembl_client",
    "default_chembl_factory",
    "RequestsTransport",
    "AioHttpTransport",
]
