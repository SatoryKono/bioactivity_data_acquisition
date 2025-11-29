from __future__ import annotations

"""Совместимость: базовый клиент перенесён в ``bioetl.clients.providers.base_provider``."""

from warnings import warn

from bioetl.clients.providers.base_provider import *  # noqa: F401,F403

warn(
    "bioetl.clients.base_provider перенесён в bioetl.clients.providers.base_provider; "
    "обновите импорты",
    DeprecationWarning,
    stacklevel=2,
)
