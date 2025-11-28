"""Совместимость: интерфейсы перенесены в ``bioetl.clients.base.interfaces``."""

from __future__ import annotations

import warnings

from bioetl.clients.base import interfaces

warnings.warn(
    "bioetl.clients.interfaces перенесён в bioetl.clients.base.interfaces; обновите импорты.",
    DeprecationWarning,
    stacklevel=2,
)

from bioetl.clients.base.interfaces import *  # noqa: F401,F403,E402

__all__ = interfaces.__all__
