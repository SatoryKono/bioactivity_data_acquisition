"""Совместимость: реестр стратегий перенесён в ``clients.factories``."""

from __future__ import annotations

import warnings

from bioetl.clients.factories.enricher_strategy_registry import *  # noqa: F401,F403

warnings.warn(
    "Импортируйте StrategyRegistry из bioetl.clients.factories.enricher_strategy_registry; старый путь устарел.",
    DeprecationWarning,
    stacklevel=2,
)
