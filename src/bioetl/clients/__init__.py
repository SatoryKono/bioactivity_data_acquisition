"""Публичное API клиентского слоя.

Обязательные правила:
- все клиенты должны реализовывать ``DataClient``;
- все вызовы клиентов используют ``ClientRequest`` и ``RequestContext``;
- слой ``clients`` не содержит доменной логики;
- старые API считаются устаревшими и должны быть удалены.
"""

from __future__ import annotations

from bioetl.clients.base.contracts import (
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
from bioetl.clients.base import exceptions as _exceptions
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
    "ClientError",
    "ClientRequest",
    "DataClient",
    "Page",
    "PageStream",
    "PaginationParams",
    "Record",
    "RecordStream",
    "RequestContext",
    "exceptions",
    "FACTORIES",
    "ClientFactory",
    "ClientProtocol",
    "register_factory",
    "register_domain_factories",
    "get_factory",
]
