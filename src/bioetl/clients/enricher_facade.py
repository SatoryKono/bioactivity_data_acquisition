from __future__ import annotations

"""Совместимость: фасад перенесён в ``bioetl.clients.factories.enricher_facade``."""

from warnings import warn

from bioetl.clients.factories.enricher_facade import *  # noqa: F401,F403

warn(
    "bioetl.clients.enricher_facade перенесён в bioetl.clients.factories.enricher_facade; "
    "обновите импорты",
    DeprecationWarning,
    stacklevel=2,
)
