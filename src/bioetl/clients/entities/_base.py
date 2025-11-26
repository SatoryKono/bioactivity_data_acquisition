from __future__ import annotations

from bioetl.clients.common import ApiTransportProtocol, PaginationStrategy, UnifiedEntityClientBase
from bioetl.infra import PaginationRegistry
from bioetl.core.http.client_mixins import ApiClientMixin, ClosableMixin


class _BaseEntityClient(UnifiedEntityClientBase):
    def __init__(
        self,
        transport: ApiTransportProtocol,
        entity: str,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_registry: PaginationRegistry | None = None,
    ) -> None:
        super().__init__(
            transport,
            entity,
            pagination_strategy=pagination_strategy,
            pagination_strategy_name=pagination_strategy_name,
            pagination_registry=pagination_registry,
        )

    def default_pagination_strategy(self, *, strategy_name: str | None = None) -> PaginationStrategy:
        return self.pagination_registry.create(strategy_name or "page_param")
