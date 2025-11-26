from __future__ import annotations

from bioetl.clients.common import ChemblClientBase
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import PaginationStrategy
from bioetl.infra import PaginationRegistry, get_default_pagination_registry


class _BaseEntityClient(ChemblClientBase):
    def __init__(
        self,
        *,
        transport: ApiTransportProtocol,
        entity: str,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_registry: PaginationRegistry | None = None,
    ) -> None:
        super().__init__(
            transport,
            entity,
            pagination_strategy=pagination_strategy,
            pagination_strategy_name=pagination_strategy_name,
            pagination_registry=pagination_registry or get_default_pagination_registry(),
        )


__all__ = ["_BaseEntityClient"]
