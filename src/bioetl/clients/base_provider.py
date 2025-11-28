"""Совместимость: базовый провайдер перемещён в ``bioetl.clients.providers.base_provider``."""

from __future__ import annotations

import warnings

from bioetl.clients.providers.base_provider import BaseDataProvider

warnings.warn(
    "bioetl.clients.base_provider перенесён в bioetl.clients.providers.base_provider; обновите импорты.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["BaseDataProvider"]
