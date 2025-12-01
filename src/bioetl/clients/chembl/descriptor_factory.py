"""Дескрипторы и фабрики для ChEMBL клиента."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence, TypeAlias


@dataclass
class BatchPlan:
    """План разбиения на батчи."""

    batch_size: int | None = None
    chunk_size: int | None = None


@dataclass
class ChemblContextFacade:
    """Фасад контекста ChEMBL."""

    transport_factory: Any
    pagination_strategy: Any
    pagination_strategy_name: str | None
    pagination_factories: Any
    chembl_release: str | None
    chembl_client: Any
    client_factory: Any


FetcherStrategy: TypeAlias = Callable[
    [Mapping[str, Any], Any], Callable[[Sequence[str] | None], Any] | None
]


@dataclass
class ChemblDescriptorFactory:
    """Фабрика дескрипторов ChEMBL."""

    context_facade: ChemblContextFacade
    fetcher_strategies: dict[str, FetcherStrategy]
    fallback_rows: Callable[
        [Any, Exception], list[dict[str, Any]]
    ] | None = None
    sort_fields: Mapping[str, Sequence[str]] | None = None
