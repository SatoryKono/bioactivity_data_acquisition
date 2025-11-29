from __future__ import annotations

"""Базовый слой клиентов внешних источников."""

from bioetl.clients.base.client_abc import BaseClient, ClientRequest, RequestContext
from bioetl.clients.base.db_backend import DbBackend
from bioetl.clients.base.exceptions import (
    ConfigurationError,
    ConnectionError,
    HTTPError,
    PaginationError,
    ProviderError,
    RequestException,
    Timeout,
)
from bioetl.clients.base.http_backend import HttpBackend
from bioetl.clients.base.paging import Page, PaginationParams
from bioetl.clients.base.types import Headers, JsonData, QueryParams, Record

__all__ = [
    "BaseClient",
    "ClientRequest",
    "RequestContext",
    "HttpBackend",
    "DbBackend",
    "Page",
    "PaginationParams",
    "Headers",
    "JsonData",
    "QueryParams",
    "Record",
    "ConfigurationError",
    "ConnectionError",
    "HTTPError",
    "PaginationError",
    "ProviderError",
    "RequestException",
    "Timeout",
]
