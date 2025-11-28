"""Совместимость: утилиты перенесены в ``clients.utils``."""

from __future__ import annotations

import warnings

from bioetl.clients.utils.common import *  # noqa: F401,F403

warnings.warn(
    "Импортируйте утилиты из bioetl.clients.utils.common; старый путь устарел.",
    DeprecationWarning,
    stacklevel=2,
)
