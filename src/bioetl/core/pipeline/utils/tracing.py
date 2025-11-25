from __future__ import annotations

import contextlib
import time
from typing import Any, Dict, Iterator

from .interfaces import TracerABC
from .logging import StructLoggerAdapter


class Span:
    """Минимальный span для трейсов."""

    def __init__(self, name: str, logger: StructLoggerAdapter, attributes: Dict[str, Any] | None = None) -> None:
        self.name = name
        self.logger = logger
        self.attributes = attributes or {}
        self.started_at = time.time()

    def __enter__(self) -> "Span":
        self.logger.info("span.start", span=self.name, **self.attributes)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        duration = time.time() - self.started_at
        payload = {**self.attributes, "duration_ms": round(duration * 1000, 2)}
        if exc:
            payload["error"] = str(exc)
            self.logger.error("span.error", span=self.name, **payload)
        else:
            self.logger.info("span.finish", span=self.name, **payload)


class SimpleTracer(TracerABC):
    """Неблокирующая реализация трассировки поверх логгера."""

    def __init__(self, logger: StructLoggerAdapter | None = None) -> None:
        self.logger = logger or StructLoggerAdapter()

    @contextlib.contextmanager
    def start_span(self, name: str, **kwargs: Any) -> Iterator[Span]:
        span = Span(name, self.logger, dict(kwargs))
        with span:
            yield span
