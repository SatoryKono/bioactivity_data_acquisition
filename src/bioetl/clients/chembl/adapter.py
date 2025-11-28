"""
Adapter for ChEMBL transport layer.

This module provides the ChemblTransportAdapter which wraps the underlying
HTTP transport to add logging, metadata capture, and pagination support.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

from bioetl.clients.chembl.pagination import (
    PaginationFactory,
    create_pagination_strategy,
)
from bioetl.core.http.adapter import LoggingTransportAdapter
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import PaginationStrategy


def _chembl_pagination_factory(
    name: str | None,
    factories: Mapping[str, Any] | None,
) -> PaginationStrategy | None:
    typed_factories = cast(
        Mapping[str, PaginationFactory] | None,
        factories,
    )
    return create_pagination_strategy(
        name,
        factories=typed_factories,
        default=None,
    )


class ChemblTransportAdapter(LoggingTransportAdapter):
    """Обёртка над транспортом ChEMBL с логированием и сбором метаданных."""

    def __init__(
        self,
        transport: ApiTransportProtocol,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_factories: Mapping[str, PaginationFactory] | None = None,
    ) -> None:
        super().__init__(
            transport,
            pagination_strategy=pagination_strategy,
            pagination_strategy_name=pagination_strategy_name,
            pagination_factory=_chembl_pagination_factory,
            pagination_factories=pagination_factories,
            client_name="chembl_transport",
        )

    def _capture_metadata(
        self, payload: Mapping[str, Any] | Sequence[Mapping[str, Any]] | Any
    ) -> None:
        """Preserve base behavior while keeping override hook for ChEMBL specifics."""

        super()._capture_metadata(payload)


__all__ = ["ChemblTransportAdapter"]
