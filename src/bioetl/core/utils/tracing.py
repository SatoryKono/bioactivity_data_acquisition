from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, ContextManager

from .interfaces import TracerABC
from .logging import StructLoggerAdapter


@dataclass
class _Span(ContextManager["_Span"]):
    name: str
    logger: StructLoggerAdapter
    metadata: dict[str, Any]
    started_at: float = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.started_at = time.perf_counter()

    def __enter__(self) -> "_Span":
        self.logger.info("span_start", span=self.name, **self.metadata)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        duration_ms = (time.perf_counter() - self.started_at) * 1000
        if exc:
            self.logger.error(
                "span_error",
                span=self.name,
                duration_ms=duration_ms,
                error_type=getattr(exc, "__class__", type(exc)).__name__,
                error_message=str(exc),
            )
        self.logger.info("span_finish", span=self.name, duration_ms=duration_ms)
        return None


class StageTracer(TracerABC):
    """Простой трейсинг стадий пайплайна через логгер."""

    def __init__(self, logger: StructLoggerAdapter | None = None) -> None:
        self._logger = logger or StructLoggerAdapter()

    def start_span(self, name: str, **kwargs: Any) -> _Span:
        return _Span(name=name, logger=self._logger, metadata=kwargs)

