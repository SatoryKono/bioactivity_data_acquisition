"""Utilities for resolving pagination strategies within ChEMBL clients."""

from __future__ import annotations

from collections.abc import Mapping

from bioetl.clients.chembl.pagination import PaginationFactory, PaginationStrategy, create_pagination_strategy
from bioetl.core.http.interfaces import ApiTransportProtocol


class PaginationStrategyResolverMixin:
    """Mixin providing a common pagination strategy resolution routine."""

    pagination_strategy: PaginationStrategy | None

    def resolve_strategy(
        self,
        transport: ApiTransportProtocol,
        *,
        name: str | None = None,
        factories: Mapping[str, PaginationFactory] | None = None,
        default: PaginationStrategy | None = None,
    ) -> PaginationStrategy:
        if default is not None:
            return default

        if name is not None or factories is not None:
            resolved = create_pagination_strategy(
                name,
                factories=factories,
                default=None,
            )
            if resolved is not None:
                return resolved

        strategy = getattr(transport, "pagination_strategy", None)
        if isinstance(strategy, PaginationStrategy):
            return strategy

        resolved = create_pagination_strategy(None, factories=factories, default=None)
        if resolved is None:
            msg = "Pagination strategy is required for ChEMBL transport"
            raise ValueError(msg)
        return resolved


__all__ = ["PaginationStrategyResolverMixin"]
