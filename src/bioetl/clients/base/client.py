from __future__ import annotations

"""Compatibility facade for client contracts."""

from .contracts import (
    ClientError,
    ClientRequest,
    DataClient,
    Page,
    PageStream,
    PaginationParams,
    Record,
    RecordStream,
    RequestContext,
)

__all__ = [
    "ClientError",
    "ClientRequest",
    "DataClient",
    "Page",
    "PageStream",
    "PaginationParams",
    "Record",
    "RecordStream",
    "RequestContext",
]
