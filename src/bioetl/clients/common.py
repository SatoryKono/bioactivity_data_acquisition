"""Common utilities and protocols for infrastructure clients."""

from __future__ import annotations

import warnings

# All imports at top level
from bioetl.core.http.api_entity_client import EntityClientProtocol
from bioetl.core.http.interfaces import (
    ApiTransportProtocol,
    BaseApiClient,
)
from bioetl.core.http.pagination import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
    NextLinkPagination,
    PageParamPagination,
    PaginationStrategy,
)
from bioetl.core.http.types import JSONPage, JSONPayload, JSONRecord, JSONRecordStream

# Warning must come before usage of deprecated imports
warnings.warn(
    "bioetl.clients.common is deprecated as source of "
    "ApiClientMixin/ClosableMixin; use bioetl.core.http "
    "instead of direct import from clients.common.",
    DeprecationWarning,
    stacklevel=2,
)


__all__ = [
    "ApiTransportProtocol",
    "BaseApiClient",
    "EntityClientProtocol",
    "JSONPage",
    "JSONPayload",
    "JSONRecord",
    "JSONRecordStream",
    "NextLinkPagination",
    "PageParamPagination",
    "PaginationStrategy",
    "DEFAULT_NEXT_KEY",
    "DEFAULT_PAGE_KEY",
    "DEFAULT_PAGE_PARAM",
]
