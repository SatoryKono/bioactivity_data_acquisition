"""BioETL Clients and Integrations."""

from bioetl.clients.exceptions import (
    ConnectionError,  # noqa: A004, pylint: disable=redefined-builtin
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
    ChemblEntityClientFactoryConfig,
    ChemblEntityClientFactoryProtocol,
    ChemblEntityClientProtocol,
)
from bioetl.clients.chembl.factories import (
    make_chembl_client,
    default_chembl_factory,
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
    "ChemblEntityClientFactoryConfig",
    "ChemblEntityClientFactoryProtocol",
    "ChemblEntityClientProtocol",
    "make_chembl_client",
    "default_chembl_factory",
]
