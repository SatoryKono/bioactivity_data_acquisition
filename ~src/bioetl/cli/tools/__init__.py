"""Совместимость с устаревшим пространством ``bioetl.cli.tools``."""

from __future__ import annotations

from typing import Any

from ._logic import LEGACY_TOOL_MAP, load_tool_module

__all__ = ["LEGACY_TOOL_MAP", "load_tool_module"]


def __getattr__(name: str) -> Any:
    """Разрешить `import bioetl.cli.tools.<tool>` через новый devtools-модуль."""

    try:
        module = load_tool_module(name)
    except ImportError as exc:  # pragma: no cover - защищаем от неверных имён
        raise AttributeError(name) from exc
    return module


def __dir__() -> list[str]:
    """Экспортировать атрибуты пакета и перечень поддерживаемых шорткатов."""

    return sorted(set(__all__ + list(LEGACY_TOOL_MAP.keys())))
