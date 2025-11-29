"""Тонкая обёртка над транспортом ChEMBL без бизнес-логики."""

from __future__ import annotations

from collections.abc import Mapping

from bioetl.clients.chembl.pagination import PaginationFactory
from bioetl.clients.chembl.strategy_resolver import (
    PaginationStrategyResolverMixin,
)
from bioetl.core.http.adapter import LoggingTransportAdapter
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import PaginationStrategy


class ChemblTransportAdapter(
    PaginationStrategyResolverMixin, LoggingTransportAdapter
):
    """Передаёт вызовы к исходному ``ApiTransportProtocol`` без дополнительных слоёв."""

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


__all__ = ["ChemblTransportAdapter"]
