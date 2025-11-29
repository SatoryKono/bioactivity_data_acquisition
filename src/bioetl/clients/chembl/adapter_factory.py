"""Factories and helpers for constructing ChEMBL transport adapters."""
from __future__ import annotations

"""Factories and helpers for constructing ChEMBL transport adapters."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from bioetl.clients.chembl.pagination import PaginationFactory
from bioetl.clients.chembl.strategy_resolver import PaginationStrategyResolverMixin
from bioetl.core.http.pagination import PaginationStrategy
from bioetl.core.http.interfaces import ApiTransportProtocol

if TYPE_CHECKING:  # pragma: no cover
    from bioetl.clients.chembl.adapter import ChemblTransportAdapter


def _get_adapter_cls():
    from bioetl.clients.chembl.adapter import ChemblTransportAdapter

    return ChemblTransportAdapter


@dataclass(slots=True)
class BaseChemblAdapterFactory(PaginationStrategyResolverMixin):
    """Создаёт адаптеры ChEMBL с учётом порядка приоритетов."""

    pagination_strategy_name: str | None = None
    pagination_strategy: PaginationStrategy | None = None
    pagination_factories: Mapping[str, PaginationFactory] | None = None
    adapter_cls: type[ApiTransportProtocol] | None = field(default=None, repr=False)

    def _resolve_strategy(self, transport: ApiTransportProtocol) -> PaginationStrategy:
        return self.resolve_strategy(
            transport,
            name=self.pagination_strategy_name,
            factories=self.pagination_factories,
            default=self.pagination_strategy,
        )

    def _get_adapter_cls(self) -> type[ApiTransportProtocol]:
        return self.adapter_cls or _get_adapter_cls()

    def create(self, transport: ApiTransportProtocol) -> ApiTransportProtocol:
        strategy = self._resolve_strategy(transport)
        adapter_cls = self._get_adapter_cls()
        return adapter_cls(
            transport,
            pagination_strategy=strategy,
            pagination_strategy_name=self.pagination_strategy_name,
            pagination_factories=self.pagination_factories,
        )

    def ensure_adapter(self, transport: ApiTransportProtocol) -> ApiTransportProtocol:
        adapter_cls = self._get_adapter_cls()
        if isinstance(transport, adapter_cls):
            base_transport = getattr(transport, "base_transport", transport)
            if self.pagination_strategy or self.pagination_strategy_name or self.pagination_factories:
                return self.create(base_transport)
            return transport
        return self.create(transport)


__all__ = [
    "BaseChemblAdapterFactory",
]
