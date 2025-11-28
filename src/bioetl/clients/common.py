"""Утилиты клиентских адаптеров и совместимые реэкспорты.

Вспомогательные функции для итерации по сущностям/страницам остаются доступны
через ``bioetl.core.http.pagination_helpers`` (реэкспортируются здесь). Все
протоколы и типы транспорта переехали в ``bioetl.core.http``; при обращении к
старым алиасам из этого модуля будет выдан ``DeprecationWarning`` и атрибут
лениво подтянется из нового местоположения.
"""

from __future__ import annotations

import importlib
import warnings
from types import ModuleType
from typing import Any

from bioetl.core.http import pagination_helpers
from bioetl.core.http.pagination_helpers import *  # noqa: F401,F403

_DEPRECATED_HTTP_EXPORTS: dict[str, tuple[str, str]] = {
    "ApiTransportProtocol": ("bioetl.core.http", "ApiTransportProtocol"),
    "BaseApiClient": ("bioetl.core.http", "BaseApiClient"),
    "EntityClientProtocol": ("bioetl.core.http", "EntityClientProtocol"),
    "JSONPage": ("bioetl.core.http", "JSONPage"),
    "JSONPayload": ("bioetl.core.http", "JSONPayload"),
    "JSONRecord": ("bioetl.core.http", "JSONRecord"),
    "JSONRecordStream": ("bioetl.core.http", "JSONRecordStream"),
    "PaginationStrategy": ("bioetl.core.http", "PaginationStrategy"),
    "NextLinkPagination": ("bioetl.core.http", "NextLinkPagination"),
    "PageParamPagination": ("bioetl.core.http", "PageParamPagination"),
}

warnings.warn(
    (
        "bioetl.clients.common устарел и будет удалён; "
        "используйте bioetl.core.http.pagination_helpers."
    ),
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [*pagination_helpers.__all__, *_DEPRECATED_HTTP_EXPORTS.keys()]


def __getattr__(name: str) -> Any:
    """Лениво реэкспортировать устаревшие атрибуты с предупреждением."""

    target = _DEPRECATED_HTTP_EXPORTS.get(name)
    if target:
        module_name, attr_name = target
        warnings.warn(
            (
                "bioetl.clients.common.%s устарел; импортируйте ``%s`` из "
                "``bioetl.core.http``."
            )
            % (name, attr_name),
            DeprecationWarning,
            stacklevel=2,
        )
        module: ModuleType = importlib.import_module(module_name)
        return getattr(module, attr_name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
