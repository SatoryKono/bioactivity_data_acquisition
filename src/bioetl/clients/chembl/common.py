from __future__ import annotations

from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import PaginationStrategy
from bioetl.infra import PaginationRegistry, get_default_pagination_registry


class ChemblPaginationMixin:
    """Миксин для настройки транспорта и стратегии пагинации ChEMBL."""

    def _init_pagination(
        self,
        transport: ApiTransportProtocol,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_registry: PaginationRegistry | None = None,
    ) -> PaginationStrategy:
        self.transport = transport
        self.pagination_registry = pagination_registry or get_default_pagination_registry()
        self.pagination_strategy = pagination_strategy or self.pagination_registry.create(
            pagination_strategy_name or "next_link"
        )
        return self.pagination_strategy


__all__ = ["ChemblPaginationMixin"]
