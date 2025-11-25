from __future__ import annotations

from bioetl.clients.common import (
    ApiTransportProtocol,
    PageParamPagination,
    PaginatedFetcher,
    PaginationStrategy,
    UnifiedEntityClientBase,
)
from bioetl.core.http.client_mixins import ApiClientMixin, ClosableMixin


class _BaseEntityClient(UnifiedEntityClientBase):
    def __init__(
        self,
        transport: ApiTransportProtocol,
        entity: str,
        *,
        pagination_strategy: PaginatedFetcher | None = None,
    ) -> None:
        super().__init__(
            transport,
            entity,
            pagination_strategy=pagination_strategy or PageParamPagination(),
        )

    def default_pagination_strategy(self) -> PaginationStrategy:
        return PageParamPagination()
