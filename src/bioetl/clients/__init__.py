"""Публичное API клиентского слоя.

Новые клиенты реализуют контракт ``ExternalDataClient`` и работают
с объектами ``ClientRequest``/``Pagination``/``RequestContext``.
Для совместимости со старым API используется модуль :mod:`bioetl.clients.legacy`.
"""

from __future__ import annotations

from bioetl.clients.base import (
    ClientRequest,
    ExternalDataClient,
    Page,
    Pagination,
    Record,
    RequestContext,
    ConfigurationError,
    ConnectionError,
    HTTPError,
    PaginationError,
    ProviderError,
    RequestException,
    Timeout,
)
from bioetl.clients.base import exceptions as _exceptions
from bioetl.clients import legacy
from bioetl.clients.registry import (
    FACTORIES,
    ClientFactory,
    ClientProtocol,
    get_factory,
    register_domain_factories,
    register_factory,
)

# Re-export exceptions for public API
exceptions = _exceptions

__all__ = [
    "ClientRequest",
    "ExternalDataClient",
    "Page",
    "Pagination",
    "Record",
    "RequestContext",
    "ConfigurationError",
    "ConnectionError",
    "HTTPError",
    "PaginationError",
    "ProviderError",
    "RequestException",
    "Timeout",
    "exceptions",
    "legacy",
    "FACTORIES",
    "ClientFactory",
    "ClientProtocol",
    "register_factory",
    "register_domain_factories",
    "get_factory",
]
