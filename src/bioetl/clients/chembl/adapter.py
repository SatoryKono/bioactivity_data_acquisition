from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import structlog

from bioetl.core.http import ApiClientMixin, ClosableMixin
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import PaginationStrategy
from bioetl.core.pipeline.unified import ChemblExtractionDescriptor
from bioetl.clients.pagination import PaginationRegistry, get_default_pagination_registry


class ChemblTransportAdapter(ApiClientMixin, ClosableMixin, ApiTransportProtocol):
    """Обёртка над транспортом ChEMBL с логированием и сбором метаданных."""

    def __init__(
        self,
        transport: ApiTransportProtocol,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_registry: PaginationRegistry | None = None,
    ) -> None:
        self._base_transport = transport
        self.transport = transport
        self.pagination_registry = pagination_registry or get_default_pagination_registry()
        self.pagination_strategy = pagination_strategy or self.pagination_registry.create(
            pagination_strategy_name or "next_link"
        )
        self._metadata: dict[str, Any] = {}
        self._logger = structlog.get_logger(__name__).bind(client="chembl_transport")

    @property
    def base_transport(self) -> ApiTransportProtocol:
        """Доступ к исходному транспорту без обёртки."""

        return self._base_transport

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
            lambda: self._base_transport.request(
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


__all__ = ["ChemblTransportAdapter", "ChemblExtractionDescriptor"]
