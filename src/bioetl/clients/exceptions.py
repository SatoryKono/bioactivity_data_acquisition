"""Совместимость: исключения перенесены в ``bioetl.clients.base.exceptions``."""

from __future__ import annotations

import warnings

from bioetl.clients.base import exceptions as _exceptions

warnings.warn(
    "bioetl.clients.exceptions перенесён в bioetl.clients.base.exceptions; обновите импорты.",
    DeprecationWarning,
    stacklevel=2,
)

from bioetl.clients.base.exceptions import *  # noqa: F401,F403,E402

__all__ = _exceptions.__all__
