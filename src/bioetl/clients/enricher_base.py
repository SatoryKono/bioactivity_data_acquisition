"""Совместимость: маршрутизатор обогащающих клиентов перенесён в подпакет ``providers``."""

from __future__ import annotations

import warnings

from bioetl.clients.providers.routes import (
    BaseEnricherClient,
    DeprecatedAliasMixin,
    EnricherClientOptions,
    EnricherClientProtocol,
    RouteConfig,
    RouteEnricherMixin,
    RouteProviderBase,
    UnifiedProviderAdapter,
    create_route_provider_class,
)

warnings.warn(
    "bioetl.clients.enricher_base перенесён в bioetl.clients.providers.routes; обновите импорты.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "BaseEnricherClient",
    "DeprecatedAliasMixin",
    "EnricherClientOptions",
    "EnricherClientProtocol",
    "RouteConfig",
    "RouteEnricherMixin",
    "RouteProviderBase",
    "UnifiedProviderAdapter",
    "create_route_provider_class",
]
