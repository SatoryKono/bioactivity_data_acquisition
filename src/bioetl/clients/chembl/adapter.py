"""Тонкая обёртка над транспортом ChEMBL без бизнес-логики."""

from __future__ import annotations

from collections.abc import Mapping

from bioetl.clients.chembl.pagination import PaginationFactory
from bioetl.clients.chembl.strategy_resolver import (
    PaginationStrategyResolverMixin,
)
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import PaginationStrategy


class ChemblTransportAdapter(PaginationStrategyResolverMixin, ApiTransportProtocol):
    """Передаёт вызовы к исходному ``ApiTransportProtocol`` без дополнительных слоёв."""

    def __init__(
        self,
        transport: ApiTransportProtocol,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_factories: Mapping[str, PaginationFactory] | None = None,
    ) -> None:
        self._base_transport = transport
        self.pagination_strategy = self.resolve_strategy(
            transport,
            name=pagination_strategy_name,
            factories=pagination_factories,
            default=pagination_strategy,
        )

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, object] | None = None,
        json: object | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Mapping[str, object] | list[Mapping[str, object]]:
        return self._base_transport.request(
            method,
            path,
            headers=headers,
            params=params,
            json=json,
            timeout_sec=timeout_sec,
            max_retries=max_retries,
        )

    @property
    def metadata(self) -> Mapping[str, object]:
        base_metadata = getattr(self._base_transport, "metadata", None)
        return dict(base_metadata) if isinstance(base_metadata, Mapping) else {}

    def close(self) -> None:
        close = getattr(self._base_transport, "close", None)
        if callable(close):
            close()


__all__ = ["ChemblTransportAdapter"]
