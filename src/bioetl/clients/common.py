"""Утилиты клиентских адаптеров и совместимые реэкспорты.

Модуль предоставляет вспомогательные функции для итерации по сущностям и
страницам API. Импортируемые ранее протоколы и типы перенесены в
``bioetl.core.http``; доступ к ним из этого модуля помечен как устаревший.
"""

from __future__ import annotations

import importlib
import warnings
from types import ModuleType
from typing import Any

from bioetl.core.http import pagination_helpers
# pylint: disable=line-too-long
from bioetl.core.http.pagination_helpers import *  # noqa: F401,F403  # type: ignore[line-too-long]  # pylint: disable=wildcard-import,unused-wildcard-import

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

__all__ = [*pagination_helpers.__all__]


def __getattr__(name: str) -> Any:
    """Лениво реэкспортировать устаревшие атрибуты с предупреждением."""

    target = _DEPRECATED_HTTP_EXPORTS.get(name)
    if target:
        module_name, attr_name = target
        warnings.warn(
            f"bioetl.clients.common.{name} устарел; "
            f"импортируйте ``{attr_name}`` из ``bioetl.core.http``.",
            DeprecationWarning,
            stacklevel=2,
        )
        module: ModuleType = importlib.import_module(module_name)
        return getattr(module, attr_name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
