from __future__ import annotations

import time
from typing import Any, MutableMapping

from .interfaces import ProgressReporterABC
from .logging import StructLoggerAdapter


class SimpleProgressReporter(ProgressReporterABC):
    """Логирование прогресса и метрик."""

    def __init__(self, logger: StructLoggerAdapter | None = None) -> None:
        self.logger = logger or StructLoggerAdapter()
        self.total: int | None = None
        self.completed = 0
        self.started_at: float | None = None

    def start(self, total: int | None = None) -> None:
        self.total = total
        self.completed = 0
        self.started_at = time.time()
        self.logger.info("progress.start", total=total)

    def advance(self, step: int = 1, metadata: MutableMapping[str, Any] | None = None) -> None:
        self.completed += step
        elapsed = time.time() - (self.started_at or time.time())
        payload = {
            "completed": self.completed,
            "total": self.total,
            "elapsed_ms": round(elapsed * 1000, 2),
        }
        if metadata:
            payload.update(metadata)
        self.logger.info("progress.tick", **payload)

    def finish(self) -> None:
        elapsed = None
        if self.started_at is not None:
            elapsed = round((time.time() - self.started_at) * 1000, 2)
        self.logger.info("progress.finish", completed=self.completed, total=self.total, elapsed_ms=elapsed)
