from __future__ import annotations

"""Совместимость: реестр стратегий перенесён в ``bioetl.clients.factories.enricher_strategy_registry``."""

from warnings import warn

from bioetl.clients.factories.enricher_strategy_registry import *  # noqa: F401,F403

warn(
    "bioetl.clients.enricher_strategy_registry перенесён в bioetl.clients.factories.enricher_strategy_registry; "
    "обновите импорты",
    DeprecationWarning,
    stacklevel=2,
)
