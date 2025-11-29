"""Утилиты клиентских адаптеров.

Вспомогательные функции для итерации по сущностям/страницам остаются доступны
через ``bioetl.core.http.pagination_helpers`` (реэкспортируются здесь).
"""

from __future__ import annotations

import warnings

from bioetl.core.http import pagination_helpers
# pylint: disable=line-too-long,wildcard-import,unused-wildcard-import
from bioetl.core.http.pagination_helpers import *  # noqa: F401,F403,E501

warnings.warn(
    (
        "bioetl.clients.common устарел и будет удалён; "
        "используйте bioetl.core.http.pagination_helpers."
    ),
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [*pagination_helpers.__all__]
