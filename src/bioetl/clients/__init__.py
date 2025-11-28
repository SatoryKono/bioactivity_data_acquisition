"""Фасад клиентского слоя BioETL."""

from __future__ import annotations

from bioetl.clients.base import (
    FACTORIES,
    ClientFactory,
    ClientProtocol,
    get_factory,
    register_domain_factories,
    register_factory,
)
from bioetl.clients.chembl import (
    BaseChemblClient,
    ChemblActivityClient,
    ChemblAssayClient,
    ChemblDocumentClient,
    ChemblEntityClientFactory,
    ChemblEntityClientFactoryConfig,
    ChemblEntityClientFactoryProtocol,
    ChemblEntityClientProtocol,
    ChemblTargetClient,
    ChemblTestItemClient,
)
from bioetl.clients.chembl.factories import (
    default_chembl_factory,
    make_chembl_client,
)
from bioetl.clients.enricher_base import (
    BaseEnricherClient,
    DeprecatedAliasMixin,
    EnricherClientOptions,
    EnricherClientProtocol,
    RouteConfig,
    RouteEnricherMixin,
    RouteProviderBase,
    create_route_provider_class,
)
from bioetl.clients.enricher_facade import (
    ClientMethodStrategy,
    EnricherFacade,
    EnrichmentStrategy,
    NullEnricherFacade,
    build_enricher_facade,
)
from bioetl.clients.enricher_factory import (
    ENRICHER_ALLOWED_ENTITIES,
    EnricherApiConfig,
    EnricherApiFactory,
    EnricherClientFactory,
    EnricherEntity,
    NULL_ENRICHER_FACTORY,
)
from bioetl.clients.enricher_strategy_registry import StrategyRegistry
from bioetl.clients.exceptions import (
    ConnectionError,  # noqa: A004, pylint: disable=redefined-builtin
    HTTPError,
    RequestException,
    Timeout,
)
from bioetl.clients.providers import (
    CrossrefClient,
    OpenAlexClient,
    PubChemClient,
    PubmedClient,
    SemanticScholarClient,
    UniProtClient,
)

__all__ = [
    # Base client plumbing
    "ConnectionError",
    "HTTPError",
    "RequestException",
    "Timeout",
    "ClientProtocol",
    "ClientFactory",
    "FACTORIES",
    "register_factory",
    "register_domain_factories",
    "get_factory",
    # Chembl clients
    "BaseChemblClient",
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
    # Enricher base
    "BaseEnricherClient",
    "DeprecatedAliasMixin",
    "EnricherClientOptions",
    "EnricherClientProtocol",
    "RouteConfig",
    "RouteEnricherMixin",
    "RouteProviderBase",
    "create_route_provider_class",
    # Enricher factory & facade
    "ClientMethodStrategy",
    "EnricherApiConfig",
    "EnricherApiFactory",
    "EnricherClientFactory",
    "EnricherEntity",
    "ENRICHER_ALLOWED_ENTITIES",
    "NULL_ENRICHER_FACTORY",
    "EnricherFacade",
    "EnrichmentStrategy",
    "NullEnricherFacade",
    "StrategyRegistry",
    "build_enricher_facade",
    # Providers
    "CrossrefClient",
    "OpenAlexClient",
    "PubChemClient",
    "PubmedClient",
    "SemanticScholarClient",
    "UniProtClient",
]
