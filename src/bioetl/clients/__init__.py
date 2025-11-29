"""Публичное API для клиентского слоя."""

# pylint: disable=redefined-builtin

from bioetl.clients.base import (
    BaseClient,
    ClientRequest,
    RequestContext,
    Page,
    PaginationParams,
    ConfigurationError,
    ConnectionError,
    HTTPError,
    PaginationError,
    ProviderError,
    RequestException,
    Timeout,
)
from bioetl.clients.factory import (
    ClientFactory,
    ConfiguredHttpClient,
    default_client_builder,
)
from bioetl.clients.registry import get_registry

__all__ = [
    "BaseClient",
    "ClientRequest",
    "RequestContext",
    "Page",
    "PaginationParams",
    "ConfigurationError",
    "ConnectionError",
    "HTTPError",
    "PaginationError",
    "ProviderError",
    "RequestException",
    "Timeout",
    "ClientFactory",
    "ConfiguredHttpClient",
    "default_client_builder",
    "get_registry",
]
