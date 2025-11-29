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
from .contracts import (
    ClientError,
    ClientRequest,
    DataClient,
    DataProviderProtocol,
    Page,
    PageStream,
    PaginationParams,
    Record,
    RecordStream,
    RequestContext,
    RetryOptions,
    SupportsBatch,
    SupportsSearch,
    TransportOptions,
)
from .registry import (
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
    "ClientError",
    "ClientRequest",
    "DataClient",
    "DataProviderProtocol",
    "SupportsBatch",
    "SupportsSearch",
    "PaginationParams",
    "RequestContext",
    "Record",
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
