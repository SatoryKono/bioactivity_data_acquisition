from __future__ import annotations

import logging
from typing import Any, Mapping

try:
    import structlog
except ImportError:  # pragma: no cover - fallback на stdlib
    structlog = None  # type: ignore

from .interfaces import LoggerAdapterABC


def configure_logging(level: int = logging.INFO) -> None:
    """Базовая настройка структурированного логирования."""

    if structlog is None:
        logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s %(message)s")
        return

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(level),
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
    )


class StructLoggerAdapter(LoggerAdapterABC):
    """Адаптер, объединяющий stdlib logging и structlog."""

    def __init__(self, logger: Any | None = None, context: Mapping[str, Any] | None = None) -> None:
        self._logger = logger or self._build_logger()
        self._context = dict(context or {})

    def bind(self, **kwargs: Any) -> "StructLoggerAdapter":
        merged = {**self._context, **kwargs}
        if structlog and hasattr(self._logger, "bind"):
            return StructLoggerAdapter(self._logger.bind(**kwargs), merged)
        return StructLoggerAdapter(self._logger, merged)

    def info(self, message: str, **kwargs: Any) -> None:
        self._log("info", message, kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        self._log("warning", message, kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        self._log("error", message, kwargs)

    def _log(self, level: str, message: str, kwargs: Mapping[str, Any]) -> None:
        payload = {**self._context, **kwargs}
        if structlog and hasattr(self._logger, level):
            getattr(self._logger, level)(message, **payload)
        else:
            getattr(self._logger, level)("%s | %s", message, payload)

    def _build_logger(self) -> Any:
        if structlog:
            return structlog.get_logger()
        return logging.getLogger("bioetl")
