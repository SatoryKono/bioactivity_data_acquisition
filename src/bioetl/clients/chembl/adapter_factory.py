"""Factories and helpers for constructing ChEMBL transport adapters."""
from __future__ import annotations

"""Factories and helpers for constructing ChEMBL transport adapters."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from bioetl.clients.chembl.pagination import (
    PaginationFactory,
    PaginationStrategy,
    create_pagination_strategy,
)
from bioetl.core.http.interfaces import ApiTransportProtocol

if TYPE_CHECKING:  # pragma: no cover
    from bioetl.clients.chembl.adapter import ChemblTransportAdapter


def resolve_pagination_strategy(
    transport: ApiTransportProtocol,
    *,
    pagination_strategy: PaginationStrategy | None = None,
    pagination_strategy_name: str | None = None,
    pagination_factories: Mapping[str, PaginationFactory] | None = None,
) -> PaginationStrategy:
    if pagination_strategy is not None:
        return pagination_strategy

    if pagination_strategy_name is not None or pagination_factories is not None:
        resolved = create_pagination_strategy(
            pagination_strategy_name,
            factories=pagination_factories,
            default=None,
        )
        if resolved is not None:
            return resolved

    strategy = getattr(transport, "pagination_strategy", None)
    if isinstance(strategy, PaginationStrategy):
        return strategy

    resolved = create_pagination_strategy(
        None, factories=pagination_factories, default=None
    )
    if resolved is None:
        msg = "Pagination strategy is required for ChEMBL transport"
        raise ValueError(msg)
    return resolved


def _get_adapter_cls():
    from bioetl.clients.chembl.adapter import ChemblTransportAdapter

    return ChemblTransportAdapter


@dataclass(slots=True)
class BaseChemblAdapterFactory:
    """Создаёт адаптеры ChEMBL с учётом порядка приоритетов."""

    pagination_strategy_name: str | None = None
    pagination_strategy: PaginationStrategy | None = None
    pagination_factories: Mapping[str, PaginationFactory] | None = None
    adapter_cls: type[ApiTransportProtocol] | None = field(default=None, repr=False)

    def _resolve_strategy(self, transport: ApiTransportProtocol) -> PaginationStrategy:
        return resolve_pagination_strategy(
            transport,
            pagination_strategy=self.pagination_strategy,
            pagination_strategy_name=self.pagination_strategy_name,
            pagination_factories=self.pagination_factories,
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
    "resolve_pagination_strategy",
]
