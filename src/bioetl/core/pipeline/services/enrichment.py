from __future__ import annotations

"""Сервисы для адаптации клиентского обогащения к pandas-слою."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pandas as pd

from bioetl.clients.enricher_facade import (
    EnricherFacade,
    EnrichmentStrategy,
    NullEnricherFacade,
    build_enricher_facade,
)
from bioetl.clients.enricher_factory import EnricherClientFactory
from bioetl.clients.enricher_strategy_registry import StrategyRegistry


@dataclass(slots=True)
class SeriesEnricher:
    """Адаптер для применения клиентского обогащения к ``pd.Series``."""

    facade: EnricherFacade | NullEnricherFacade

    def enrich(self, column: pd.Series | Any, client_name: str) -> pd.Series:
        series = column if isinstance(column, pd.Series) else pd.Series(column)

        if series.empty:
            return pd.Series([None] * len(series), index=series.index, dtype=object)

        def apply(value: Any) -> Any:
            if pd.isna(value):
                return None
            return self.facade.enrich(value, client_name)

        return series.apply(apply)


def build_series_enricher(
    enricher_factory: EnricherClientFactory | None,
    strategies: StrategyRegistry | Mapping[str, EnrichmentStrategy] | None,
) -> SeriesEnricher:
    """Построить адаптер для работы с ``pd.Series`` на основе клиентов."""

    facade = build_enricher_facade(enricher_factory, strategies)
    return SeriesEnricher(facade)


__all__ = [
    "SeriesEnricher",
    "build_series_enricher",
]
