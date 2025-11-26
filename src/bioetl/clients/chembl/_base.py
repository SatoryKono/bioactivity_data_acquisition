from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import structlog

from bioetl.clients.common import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
    ApiTransportProtocol,
    PaginationStrategy,
    UnifiedEntityClientBase,
)
from bioetl.core.http.client_mixins import ApiClientMixin, ClosableMixin
from bioetl.core.pipeline.unified import ChemblExtractionDescriptor
from bioetl.infra import PaginationRegistry, get_default_pagination_registry


class BaseChemblClient(ApiClientMixin, ClosableMixin, ApiTransportProtocol):
    """Транспортный клиент ChEMBL без привязки к конкретной сущности."""

    def __init__(self, transport: ApiTransportProtocol) -> None:
        self.transport = transport
        self._logger = structlog.get_logger(__name__).bind(client="chembl_transport")

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
    ) -> Mapping[str, Any] | Sequence[Mapping[str, Any]]:
        return self._wrap_callable(
            lambda: self.transport.request(
                method,
                path,
                headers=headers,
                params=params,
                json=json,
            ),
            log_context={"path": path, "method": method},
        )


class ChemblEntityClient(UnifiedEntityClientBase):
    def __init__(
        self,
        transport: ApiTransportProtocol,
        entity: str,
        *,
        pagination_strategy_name: str | None = None,
        pagination_registry: PaginationRegistry | None = None,
    ) -> None:
        pagination_registry = pagination_registry or get_default_pagination_registry()
        super().__init__(
            transport,
            entity,
            pagination_strategy_name=pagination_strategy_name,
            pagination_registry=pagination_registry,
        )

    def default_pagination_strategy(self, *, strategy_name: str | None = None) -> PaginationStrategy:
        return self.pagination_registry.create(strategy_name or "next_link")


__all__ = ["BaseChemblClient", "ChemblEntityClient"]
