from __future__ import annotations

"""Совместимость: публичные протоколы перенесены в ``bioetl.clients.base``."""

from warnings import warn

from bioetl.clients.base import *  # noqa: F401,F403

warn(
    "Модуль bioetl.clients.base перенесён в пакет bioetl.clients.base.*; "
    "обновите импорты",
    DeprecationWarning,
    stacklevel=2,
)
