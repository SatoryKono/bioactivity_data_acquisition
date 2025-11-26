from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import structlog

from bioetl.clients.chembl.common import ChemblPaginationMixin
from bioetl.core.http import ApiClientMixin, ClosableMixin
from bioetl.core.http.api_entity_client import BaseApiEntityClient
from bioetl.core.http.entity_helpers import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
)
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import PaginationStrategy
from bioetl.core.pipeline.unified import ChemblExtractionDescriptor
from bioetl.infra import PaginationRegistry


class BaseChemblClient(ChemblPaginationMixin, ApiClientMixin, ClosableMixin, ApiTransportProtocol):
    """Транспортный клиент ChEMBL без привязки к конкретной сущности."""

    def __init__(
        self,
        transport: ApiTransportProtocol,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_registry: PaginationRegistry | None = None,
    ) -> None:
        self._init_pagination(
            transport,
            pagination_strategy=pagination_strategy,
            pagination_strategy_name=pagination_strategy_name,
            pagination_registry=pagination_registry,
        )
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


class ChemblEntityClient(ChemblPaginationMixin, BaseApiEntityClient):
    def __init__(
        self,
        transport: ApiTransportProtocol,
        entity: str,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_registry: PaginationRegistry | None = None,
    ) -> None:
        pagination = self._init_pagination(
            transport,
            pagination_strategy=pagination_strategy,
            pagination_strategy_name=pagination_strategy_name,
            pagination_registry=pagination_registry,
        )
        super().__init__(self.transport, pagination, entity=entity)

    def status(self) -> Mapping[str, Any]:
        """Check ChEMBL API status."""
        return self._wrap_callable(
            lambda: self._transport().request("GET", "/status"),
            log_context={"path": "/status", "method": "GET"},
        )  # type: ignore[return-value]


__all__ = ["BaseChemblClient", "ChemblEntityClient"]
