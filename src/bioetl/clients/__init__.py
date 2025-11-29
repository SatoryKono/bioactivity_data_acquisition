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
