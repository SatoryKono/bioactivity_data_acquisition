"""Совместимость с прежними контрактами клиентского слоя."""

from bioetl.clients.base.legacy_contracts import (
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

__all__ = [
    "ClientError",
    "ClientRequest",
    "DataClient",
    "DataProviderProtocol",
    "Page",
    "PageStream",
    "PaginationParams",
    "Record",
    "RecordStream",
    "RequestContext",
    "RetryOptions",
    "SupportsBatch",
    "SupportsSearch",
    "TransportOptions",
]
