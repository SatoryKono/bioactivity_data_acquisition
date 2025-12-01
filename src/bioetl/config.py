from __future__ import annotations

import warnings

from bioetl.clients.config.loader import load_source_config
from bioetl.clients.config.models import SourceConfig


def load_client_config(source: str) -> SourceConfig:
    """Совместимый алиас к ``bioetl.clients.config.loader.load_source_config``.

    Исторический загрузчик из ``bioetl.config`` оставлен для кода, который ещё
    не мигрировал на новый путь. Теперь он просто проксирует вызов к
    ``load_source_config`` и возвращает типизированный ``SourceConfig``.
    """

    warnings.warn(
        "bioetl.config.load_client_config устарел; используйте "
        "bioetl.clients.config.loader.load_source_config",
        DeprecationWarning,
        stacklevel=2,
    )
    return load_source_config(source)


__all__ = ["load_client_config"]
