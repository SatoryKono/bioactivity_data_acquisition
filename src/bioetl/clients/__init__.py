"""Backward-compat shim for bioetl.clients -> bioetl.infrastructure.clients"""

from __future__ import annotations

from bioetl.infrastructure.clients.client_exceptions import (
    ConnectionError,
    HTTPError,
    RequestException,
    Timeout,
)
from bioetl.infrastructure.clients.common import (
    ApiTransportProtocol,
    EntityClientProtocol,
    NextLinkPagination,
    PageParamPagination,
    PaginationStrategy,
    cache_entity_client,
)
from bioetl.infrastructure.clients.chembl import (
    ChemblActivityClient,
    ChemblAssayClient,
    ChemblDocumentClient,
    ChemblTargetClient,
    ChemblTestItemClient,
)
from bioetl.infrastructure.clients.entities.common import (
    CHEMBL_ALLOWED_ENTITIES,
)
from bioetl.infrastructure.clients.entities import (
    ChemblEntityClientFactory,
)
from bioetl.infrastructure.clients.transports import (
    AioHttpTransport,
    RequestsTransport,
)
from bioetl.infrastructure.clients.factories import (
    default_chembl_factory,
)
from bioetl.infra import PaginationRegistry
import warnings

# Issue deprecation warning on first import
warnings.warn(
    "Importing from 'bioetl.clients' is deprecated; "
    "use 'bioetl.infrastructure.clients' instead",
    DeprecationWarning,
    stacklevel=2,
)


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
