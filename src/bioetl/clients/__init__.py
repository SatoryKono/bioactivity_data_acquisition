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
from bioetl.clients.entities import ChemblEntityClientFactory
from bioetl.clients.entities.common import CHEMBL_ALLOWED_ENTITIES
from bioetl.clients.transports import AioHttpTransport, RequestsTransport
from bioetl.clients.factories import default_chembl_factory
from bioetl.infra import PaginationRegistry


def make_chembl_client(
    entity: str,
    transport: ApiTransportProtocol,
    *,
    pagination_strategy: PaginationStrategy | None = None,
    pagination_strategy_name: str | None = None,
    pagination_registry: PaginationRegistry | None = None,
):
    """Построить клиента ChEMBL для разрешённой сущности.

    Поддерживает явную валидацию сущностей через ``CHEMBL_ALLOWED_ENTITIES``.
    """

    if entity not in CHEMBL_ALLOWED_ENTITIES:
        msg = f"Unsupported ChEMBL entity: {entity}"
        raise ValueError(msg)

    factory = ChemblEntityClientFactory(
        lambda: transport,
        pagination_strategy=pagination_strategy,
        pagination_strategy_name=pagination_strategy_name,
        pagination_registry=pagination_registry,
    )
    return factory.create(entity)

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
    "CHEMBL_ALLOWED_ENTITIES",
    "make_chembl_client",
    "default_chembl_factory",
    "RequestsTransport",
    "AioHttpTransport",
]
