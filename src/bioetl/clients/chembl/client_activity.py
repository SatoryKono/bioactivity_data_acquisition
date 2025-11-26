from __future__ import annotations

from typing import Any

from bioetl.clients.chembl._base import ChemblEntityClient
from bioetl.core.http.interfaces import ApiTransportProtocol


class ChemblActivityClient(ChemblEntityClient):
    def __init__(
        self,
        transport: ApiTransportProtocol,
        *,
        pagination_strategy: Any | None = None,
        pagination_strategy_name: str | None = None,
        pagination_registry: Any | None = None,
    ) -> None:
        super().__init__(
            transport,
            "activity",
            pagination_strategy=pagination_strategy,
            pagination_strategy_name=pagination_strategy_name,
            pagination_registry=pagination_registry,
        )


__all__ = ["ChemblActivityClient"]
