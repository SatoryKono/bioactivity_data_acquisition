from __future__ import annotations

from typing import Any, MutableMapping

from .interfaces import ProgressReporterABC
from .logging import StructLoggerAdapter


class SimpleProgressReporter(ProgressReporterABC):
    """Логирует прогресс выполнения этапов и метрики."""

    def __init__(self, logger: StructLoggerAdapter | None = None) -> None:
        self._logger = logger or StructLoggerAdapter()
        self._total: int | None = None
        self._current = 0
        self._active = False

    def start(self, total: int | None = None) -> None:
        self._total = total
        self._current = 0
        self._active = True
        self._logger.info("progress_start", total=total)

    def advance(self, step: int = 1, metadata: MutableMapping[str, Any] | None = None) -> None:
        if not self._active:
            self.start()
        self._current += step
        payload = {"current": self._current, "total": self._total}
        if metadata:
            payload.update(metadata)
        self._logger.info("progress_advance", **payload)

    def finish(self) -> None:
        if not self._active:
            return
        self._logger.info("progress_finish", current=self._current, total=self._total)
        self._active = False

