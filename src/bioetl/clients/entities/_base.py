from __future__ import annotations

from bioetl.clients.common import (
    ApiTransportProtocol,
    PaginatedFetcher,
    PaginationStrategy,
    UnifiedEntityClientBase,
)
from bioetl.core.http.client_mixins import ApiClientMixin, ClosableMixin
from bioetl.infra.pagination_registry import PaginationRegistry


class _BaseEntityClient(UnifiedEntityClientBase):
    def __init__(
        self,
        transport: ApiTransportProtocol,
        entity: str,
        *,
        pagination_strategy: PaginatedFetcher | None = None,
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

    def default_pagination_strategy_name(self) -> str:
        return "page_param"
