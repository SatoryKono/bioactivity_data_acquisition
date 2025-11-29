from __future__ import annotations

"""Совместимость: фабрика перенесена в ``bioetl.clients.factories.enricher_factory``."""

from warnings import warn

from bioetl.clients.factories.enricher_factory import *  # noqa: F401,F403

warn(
    "bioetl.clients.enricher_factory перенесён в bioetl.clients.factories.enricher_factory; "
    "обновите импорты",
    DeprecationWarning,
    stacklevel=2,
)
