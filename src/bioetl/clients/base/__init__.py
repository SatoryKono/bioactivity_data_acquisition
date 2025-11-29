from __future__ import annotations

"""Базовые абстракции клиентского слоя."""

from .exceptions import (
    ConfigurationError,
    ConnectionError,
    HTTPError,
    PaginationError,
    ProviderError,
    RequestException,
    Timeout,
)
from .interfaces import (
    DataProviderProtocol,
    Page,
    PageStream,
    PaginationParams,
    RecordStream,
    RequestContext,
    RetryOptions,
    SupportsBatch,
    SupportsSearch,
    TransportOptions,
)
from bioetl.clients.registry import (
    FACTORIES,
    ClientFactory,
    ClientProtocol,
    get_factory,
    register_domain_factories,
    register_factory,
)

__all__ = [
    "ConfigurationError",
    "ConnectionError",
    "HTTPError",
    "PaginationError",
    "ProviderError",
    "RequestException",
    "Timeout",
    "DataProviderProtocol",
    "SupportsBatch",
    "SupportsSearch",
    "PaginationParams",
    "RequestContext",
    "RecordStream",
    "Page",
    "PageStream",
    "TransportOptions",
    "RetryOptions",
    "FACTORIES",
    "ClientFactory",
    "ClientProtocol",
    "register_factory",
    "register_domain_factories",
    "get_factory",
]
