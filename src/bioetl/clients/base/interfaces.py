from __future__ import annotations

"""Compatibility facade for legacy provider interfaces."""

from .contracts import (
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

__all__ = [
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
]
