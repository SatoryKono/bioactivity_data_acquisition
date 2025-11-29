from __future__ import annotations

"""Совместимость: реэкспорт исключений из ``bioetl.clients.base.exceptions``."""

from warnings import warn

from bioetl.clients.base.exceptions import *  # noqa: F401,F403

warn(
    "bioetl.clients.exceptions перенесён в bioetl.clients.base.exceptions; "
    "используйте новый путь",
    DeprecationWarning,
    stacklevel=2,
)
