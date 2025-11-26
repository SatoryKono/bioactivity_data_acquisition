from __future__ import annotations

from bioetl.clients.common import BaseApiEntityClient, PaginationStrategy
from bioetl.clients.common import ApiTransportProtocol
from bioetl.infra import PaginationRegistry, get_default_pagination_registry


class _BaseEntityClient(BaseApiEntityClient):
    """Базовая реализация клиента сущности для унифицированного транспорта."""

    def __init__(
        self,
        transport: ApiTransportProtocol,
        entity: str,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_registry: PaginationRegistry | None = None,
    ) -> None:
        registry = pagination_registry or get_default_pagination_registry()
        pagination = pagination_strategy or registry.create(pagination_strategy_name or "page_param")
        self.pagination_registry = registry
        super().__init__(transport, pagination, entity=entity)

    def default_pagination_strategy(self, *, strategy_name: str | None = None) -> PaginationStrategy:
        """Выбор стратегии пагинации через реестр (по умолчанию ``page_param``)."""

        return self.pagination_registry.create(strategy_name or "page_param")


__all__ = ["_BaseEntityClient"]
