from __future__ import annotations

"""Базовые абстракции клиентского слоя.

Новые контракты доступны напрямую из этого модуля.
Совместимость с легаси-API сохраняется через alias-переменные,
а также модуль ``bioetl.clients.legacy``.
"""

from .contracts import (
    ClientRequest,
    ExternalDataClient,
    Page,
    Pagination,
    Record,
    RequestContext,
)
from .http_backend import HttpBackend
from .rest_client import ConfiguredRestClient
from .exceptions import (
    ConfigurationError,
    ConnectionError,
    HTTPError,
    PaginationError,
    ProviderError,
    RequestException,
    Timeout,
)
from .legacy_contracts import (
    ClientError,
    ClientRequest as _LegacyClientRequest,
    DataClient,
    DataProviderProtocol,
    Page as _LegacyPage,
    PageStream as LegacyPageStream,
    PaginationParams,
    Record as _LegacyRecord,
    RecordStream as LegacyRecordStream,
    RequestContext as _LegacyRequestContext,
    RetryOptions,
    SupportsBatch,
    SupportsSearch,
    TransportOptions,
)

LegacyClientRequest = _LegacyClientRequest
LegacyRequestContext = _LegacyRequestContext
LegacyPage = _LegacyPage
LegacyRecord = _LegacyRecord

__all__ = [
    "ClientRequest",
    "ExternalDataClient",
    "Page",
    "Pagination",
    "Record",
    "RequestContext",
    "ConfiguredRestClient",
    "HttpBackend",
    "ConfigurationError",
    "ConnectionError",
    "HTTPError",
    "PaginationError",
    "ProviderError",
    "RequestException",
    "Timeout",
    # legacy exports
    "LegacyClientRequest",
    "LegacyRequestContext",
    "LegacyPage",
    "LegacyPageStream",
    "LegacyRecord",
    "LegacyRecordStream",
    "PaginationParams",
    "DataClient",
    "DataProviderProtocol",
    "SupportsBatch",
    "SupportsSearch",
    "TransportOptions",
    "RetryOptions",
    "ClientError",
]
