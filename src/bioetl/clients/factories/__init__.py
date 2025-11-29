from __future__ import annotations

"""Фабричный слой клиентских адаптеров."""

from .enricher_facade import (
    ClientMethodStrategy,
    EnricherFacade,
    EnrichmentStrategy,
    NullEnricherFacade,
    build_enricher_facade,
)
from .enricher_factory import (
    ENRICHER_ALLOWED_ENTITIES,
    EnricherApiConfig,
    EnricherApiFactory,
    EnricherClientFactory,
    EnricherEntity,
    NULL_ENRICHER_FACTORY,
)
from .enricher_strategy_registry import StrategyRegistry

__all__ = [
    "ClientMethodStrategy",
    "EnricherFacade",
    "EnrichmentStrategy",
    "NullEnricherFacade",
    "build_enricher_facade",
    "ENRICHER_ALLOWED_ENTITIES",
    "EnricherApiConfig",
    "EnricherApiFactory",
    "EnricherClientFactory",
    "EnricherEntity",
    "NULL_ENRICHER_FACTORY",
    "StrategyRegistry",
]
