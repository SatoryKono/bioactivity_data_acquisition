from __future__ import annotations

from bioetl.base_classes import BaseApiClient
from bioetl.clients.common import PageParamPagination, PaginationStrategy, UnifiedEntityClientBase


class _BaseEntityClient(UnifiedEntityClientBase):
    def __init__(
        self,
        api_client: BaseApiClient,
        entity: str,
        *,
        pagination_strategy: PaginationStrategy | None = None,
    ) -> None:
        super().__init__(api_client, entity, pagination_strategy=pagination_strategy)

    def default_pagination_strategy(self) -> PaginationStrategy:
        return PageParamPagination()
