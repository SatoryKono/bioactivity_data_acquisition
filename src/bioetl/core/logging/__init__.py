from __future__ import annotations

import logging
from typing import Any, Mapping


class UnifiedLogger:
    """Минимальный адаптер вокруг стандартного логгера.

    Поддерживает метод ``bind`` для добавления контекста и единообразные
    методы уровней логирования. Используется как точка расширения для
    структурированного логирования.
    """

    def __init__(self, logger: logging.Logger, context: Mapping[str, Any] | None = None) -> None:
        self._logger = logger
        self._context: dict[str, Any] = dict(context or {})

    @classmethod
    def get(cls, name: str) -> "UnifiedLogger":
        logging.basicConfig(level=logging.INFO)
        return cls(logging.getLogger(name))

    def bind(self, **context: Any) -> "UnifiedLogger":
        merged = dict(self._context)
        merged.update(context)
        return UnifiedLogger(self._logger, merged)

    def _log(self, level: int, event: str, **kwargs: Any) -> None:
        extra = {"event": event, **self._context, **kwargs}
        self._logger.log(level, event, extra=extra)

    def info(self, event: str, **kwargs: Any) -> None:
        self._log(logging.INFO, event, **kwargs)

    def warning(self, event: str, **kwargs: Any) -> None:
        self._log(logging.WARNING, event, **kwargs)

    def error(self, event: str, **kwargs: Any) -> None:
        self._log(logging.ERROR, event, **kwargs)

    def debug(self, event: str, **kwargs: Any) -> None:
        self._log(logging.DEBUG, event, **kwargs)


class LogEvents:
    API_CALL = "api_call"
    RETRY = "api_retry"
    CACHE_HIT = "cache_hit"
    CACHE_MISS = "cache_miss"
    CIRCUIT_BREAKER = "circuit_breaker"
    RATE_LIMIT = "rate_limit"


__all__ = ["UnifiedLogger", "LogEvents"]
