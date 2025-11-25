from __future__ import annotations

import logging
from typing import Any

import structlog

from .interfaces import LoggerAdapterABC


def configure_structlog(level: int = logging.INFO) -> None:
    """Стандартная настройка структурированного логирования для пайплайнов."""

    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            timestamper,
            structlog.processors.add_log_level,
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        cache_logger_on_first_use=True,
    )


class StructLoggerAdapter(LoggerAdapterABC):
    """Адаптер над ``structlog`` с единым интерфейсом логирования."""

    def __init__(self, *, logger: structlog.stdlib.BoundLogger | None = None, **context: Any) -> None:
        base_logger = logger or structlog.get_logger()
        self._logger = base_logger.bind(**context) if context else base_logger

    def bind(self, **context: Any) -> "StructLoggerAdapter":
        return StructLoggerAdapter(logger=self._logger.bind(**context))

    def info(self, message: str, **kwargs: Any) -> None:
        self._logger.info(message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        self._logger.warning(message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        self._logger.error(message, **kwargs)

    def debug(self, message: str, **kwargs: Any) -> None:
        self._logger.debug(message, **kwargs)

