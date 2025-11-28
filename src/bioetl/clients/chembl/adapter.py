"""
Adapter for ChEMBL transport layer.

This module provides the ChemblTransportAdapter which wraps the underlying
HTTP transport to add logging, metadata capture, and pagination support.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from bioetl.clients.chembl.pagination import PaginationFactory
from bioetl.clients.chembl.strategy_resolver import PaginationStrategyResolverMixin
from bioetl.core.http.adapter import LoggingTransportAdapter
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import PaginationStrategy

class ChemblTransportAdapter(PaginationStrategyResolverMixin, LoggingTransportAdapter):
    """Обёртка над транспортом ChEMBL с логированием и сбором метаданных."""

    def __init__(
        self,
        transport: ApiTransportProtocol,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_factories: Mapping[str, PaginationFactory] | None = None,
    ) -> None:
        strategy = self.resolve_strategy(
            transport,
            name=pagination_strategy_name,
            factories=pagination_factories,
            default=pagination_strategy,
        )
        super().__init__(
            transport,
            pagination_strategy=strategy,
            pagination_strategy_name=pagination_strategy_name,
            pagination_factories=pagination_factories,
            client_name="chembl_transport",
        )

    def _capture_metadata(
        self, payload: Mapping[str, Any] | Sequence[Mapping[str, Any]] | Any
    ) -> None:
        """Preserve base behavior while keeping override hook for ChEMBL specifics."""

        super()._capture_metadata(payload)


__all__ = ["ChemblTransportAdapter"]
