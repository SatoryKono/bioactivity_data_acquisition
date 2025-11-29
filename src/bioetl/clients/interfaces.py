from __future__ import annotations

"""Совместимость: реэкспорт базовых интерфейсов из ``bioetl.clients.base``.

Новый стабильный путь: :mod:`bioetl.clients.base.interfaces`.
"""

from warnings import warn

from bioetl.clients.base.interfaces import *  # noqa: F401,F403

warn(
    "bioetl.clients.interfaces перенесён в bioetl.clients.base.interfaces; "
    "используйте новый путь",
    DeprecationWarning,
    stacklevel=2,
)
