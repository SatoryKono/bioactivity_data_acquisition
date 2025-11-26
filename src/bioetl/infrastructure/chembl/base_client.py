from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from bioetl.core.http.api_entity_client import BaseApiEntityClient
from bioetl.core.http.entity_helpers import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
)
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import PaginationStrategy
from bioetl.infrastructure.chembl.transport_adapter import ChemblTransportAdapter
from bioetl.infra import PaginationRegistry


class BaseChemblClient(ChemblTransportAdapter):
    """Совместимый транспортный клиент ChEMBL поверх произвольного транспорта."""


class ChemblEntityClient(BaseApiEntityClient):
    def __init__(
        self,
        transport: ApiTransportProtocol,
        entity: str,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_registry: PaginationRegistry | None = None,
    ) -> None:
        adapter = ChemblTransportAdapter(
            transport,
            pagination_strategy=pagination_strategy,
            pagination_strategy_name=pagination_strategy_name,
            pagination_registry=pagination_registry,
        )
        super().__init__(adapter, adapter.pagination_strategy, entity=entity)

    @property
    def metadata(self) -> Mapping[str, Any]:
        base_transport = getattr(self, "transport", None)
        if base_transport is None:
            return {}
        metadata = getattr(base_transport, "metadata", None)
        if isinstance(metadata, Mapping):
            return dict(metadata)
        return {}

    def status(self) -> Mapping[str, Any]:
        """Check ChEMBL API status."""
        return self._wrap_callable(
            lambda: self._transport().request("GET", "/status"),
            log_context={"path": "/status", "method": "GET"},
        )  # type: ignore[return-value]


__all__ = [
    "BaseChemblClient",
    "ChemblEntityClient",
    "DEFAULT_PAGE_KEY",
    "DEFAULT_NEXT_KEY",
    "DEFAULT_PAGE_PARAM",
]
