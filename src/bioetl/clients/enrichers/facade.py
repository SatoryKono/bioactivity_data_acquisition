from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

import pandas as pd
import structlog

from bioetl.clients.enrichers.factory import (
    EnricherClientFactory,
    NULL_ENRICHER_FACTORY,
)

if TYPE_CHECKING:
    from bioetl.clients.enrichers.strategy_registry import StrategyRegistry


class EnrichmentStrategy(Protocol):
    """Стратегия обогащения для конкретного клиента."""

    def enrich(self, value: Any, factory: EnricherClientFactory) -> Any:
        ...
@dataclass
class ClientMethodStrategy:
    """Базовая стратегия вызова метода клиента.

    Args:
        client_provider: Callable, возвращающая клиент обогащения. Может
            использовать ``factory`` или игнорировать его и возвращать заранее
            подготовленный клиент.
        method_name: Имя вызываемого метода клиента.
        cache_client: Кэшировать ли результат ``client_provider`` для повторных
            вызовов. Это полезно, если клиент создаётся дорого или если
            требуется контроль над количеством HTTP-сеансов.
    """

    client_provider: Callable[[EnricherClientFactory], Any]
    method_name: str
    cache_client: bool = True

    def __post_init__(self) -> None:
        self._cached_client: Any | None = None

    def _get_client(self, factory: EnricherClientFactory) -> Any:
        if self.cache_client and self._cached_client is not None:
            return self._cached_client

        client = self.client_provider(factory)
        if self.cache_client:
            self._cached_client = client
        return client

    def enrich(self, value: Any, factory: EnricherClientFactory) -> Any:
        client = self._get_client(factory)
        method = getattr(client, self.method_name, None)
        if not callable(method):  # pragma: no cover - defensive branch
            raise AttributeError(
                f"Client {client!r} has no callable '{self.method_name}'",
            )
        return method(value)


class EnricherFacade:
    """Фасад для единообразного обогащения колонок DataFrame."""

    def __init__(
        self,
        enricher_factory: EnricherClientFactory,
        strategies: Mapping[str, EnrichmentStrategy],
    ) -> None:
        self._factory = enricher_factory
        self._strategies = dict(strategies)
        self._logger = structlog.get_logger(__name__)

    def enrich(self, column: pd.Series, client_name: str) -> pd.Series:
        """Обогатить серию, обрабатывая пустые значения и ошибки.

        Возвращает новую серию с результатами обогащения; пустые значения и
        исключения приводят к ``None`` в результирующей колонке.
        """

        series = column if isinstance(column, pd.Series) else pd.Series(column)

        if series.empty:
            return pd.Series([None] * len(series), index=series.index, dtype=object)

        strategy = self._strategies.get(client_name)
        if strategy is None:
            self._logger.warning(
                "enrichment_strategy_missing", client=client_name
            )
            return pd.Series([None] * len(series), index=series.index, dtype=object)

        def apply(value: Any) -> Any:
            if pd.isna(value):
                return None
            try:
                return strategy.enrich(value, self._factory)
            except Exception as exc:  # pragma: no cover - safety net
                self._logger.warning(
                    "enrichment_failed", client=client_name, error=str(exc)
                )
                return None

        return series.apply(apply)


class NullEnricherFacade:
    """Заглушка фасада: всегда возвращает ``None`` для обогащения."""

    def enrich(self, column: pd.Series, client_name: str) -> pd.Series:  # noqa: D401
        _ = client_name
        series = column if isinstance(column, pd.Series) else pd.Series(column)
        return pd.Series([None] * len(series), index=series.index, dtype=object)


def build_enricher_facade(
    enricher_factory: EnricherClientFactory | None,
    strategies: Mapping[str, EnrichmentStrategy] | "StrategyRegistry" | None,
) -> EnricherFacade | NullEnricherFacade:
    """Собрать ``EnricherFacade`` из фабрики и реестра стратегий."""

    if not isinstance(enricher_factory, EnricherClientFactory):
        return NullEnricherFacade()

    strategies_map = dict(strategies or {}) if isinstance(strategies, Mapping) else {}
    if not strategies_map:
        return NullEnricherFacade()

    return EnricherFacade(enricher_factory, strategies_map)


__all__ = [
    "ClientMethodStrategy",
    "EnricherFacade",
    "EnrichmentStrategy",
    "NullEnricherFacade",
    "build_enricher_facade",
    "NULL_ENRICHER_FACTORY",
]
