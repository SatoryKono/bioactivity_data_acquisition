from __future__ import annotations

"""Совместимость: утилиты перенесены в ``bioetl.clients.utils.common``."""

from warnings import warn

from bioetl.clients.utils.common import *  # noqa: F401,F403

warn(
    "bioetl.clients.common перенесён в bioetl.clients.utils.common; обновите импорты",
    DeprecationWarning,
    stacklevel=2,
)
