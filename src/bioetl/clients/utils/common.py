"""Утилиты клиентских адаптеров.

Вспомогательные функции для итерации по сущностям/страницам остаются доступны
через ``bioetl.core.http.pagination_helpers`` (реэкспортируются здесь).
"""

from __future__ import annotations

import warnings

from bioetl.core.http import pagination_helpers
# pylint: disable=line-too-long,wildcard-import,unused-wildcard-import
from bioetl.core.http.pagination_helpers import *  # noqa: F401,F403,E501

PAGINATION_META_KEYS: tuple[str, ...] = ()


def ensure_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]

warnings.warn(
    (
        "bioetl.clients.common устарел и будет удалён; "
        "используйте bioetl.core.http.pagination_helpers."
    ),
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [*pagination_helpers.__all__, "PAGINATION_META_KEYS", "ensure_list"]
