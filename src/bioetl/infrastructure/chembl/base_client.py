from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import structlog

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
from bioetl.infra import PaginationRegistry, get_default_pagination_registry


class ChemblPaginationMixin:
    """Миксин для настройки транспорта и стратегии пагинации ChEMBL."""

    def _init_pagination(
        self,
        transport: ApiTransportProtocol,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_registry: PaginationRegistry | None = None,
    ) -> PaginationStrategy:
        self.transport = transport
        self.pagination_registry = pagination_registry or get_default_pagination_registry()
        self.pagination_strategy = pagination_strategy or self.pagination_registry.create(
            pagination_strategy_name or "next_link"
        )
        return self.pagination_strategy


class BaseChemblClient(
    ChemblPaginationMixin, ApiClientMixin, ClosableMixin, ApiTransportProtocol
):
    """Транспортный клиент ChEMBL без привязки к конкретной сущности."""

    def __init__(
        self,
        transport: ApiTransportProtocol,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_registry: PaginationRegistry | None = None,
    ) -> None:
        self._metadata: dict[str, Any] = {}
        self._init_pagination(
            transport,
            pagination_strategy=pagination_strategy,
            pagination_strategy_name=pagination_strategy_name,
            pagination_registry=pagination_registry,
        )
        self._logger = structlog.get_logger(__name__).bind(client="chembl_transport")

    @property
    def metadata(self) -> Mapping[str, Any]:
        return dict(self._metadata)

    def _capture_metadata(
        self, payload: Mapping[str, Any] | Sequence[Mapping[str, Any]] | Any
    ) -> None:
        if not isinstance(payload, Mapping):
            return

        collected: dict[str, Any] = {}
        for key in ("page_meta", "meta"):
            value = payload.get(key)
            if isinstance(value, Mapping):
                collected.update(value)

        if collected:
            self._metadata.update(collected)

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
    ) -> Mapping[str, Any] | Sequence[Mapping[str, Any]]:
        response = self._wrap_callable(
            lambda: self.transport.request(
                method,
                path,
                headers=headers,
                params=params,
                json=json,
            ),
            log_context={"path": path, "method": method},
        )
        self._capture_metadata(response)
        return response


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
    "ChemblPaginationMixin",
    "BaseChemblClient",
    "ChemblEntityClient",
    "DEFAULT_PAGE_KEY",
    "DEFAULT_NEXT_KEY",
    "DEFAULT_PAGE_PARAM",
    "ChemblExtractionDescriptor",
]
