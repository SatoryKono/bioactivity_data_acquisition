from __future__ import annotations

from bioetl.base_classes import BaseApiClient
from bioetl.clients.chembl._base import BaseChemblClient
from bioetl.infra import PaginationRegistry


class ChemblTargetClient(BaseChemblClient):
    def __init__(
        self,
        api_client: BaseApiClient,
        *,
        pagination_strategy_name: str | None = None,
        pagination_registry: PaginationRegistry | None = None,
    ) -> None:
        super().__init__(
            api_client,
            "target",
            pagination_strategy_name=pagination_strategy_name,
            pagination_registry=pagination_registry,
        )


__all__ = ["ChemblTargetClient"]
