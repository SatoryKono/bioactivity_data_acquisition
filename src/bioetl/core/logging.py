"""Unified logging helpers for BioETL."""
from __future__ import annotations

import json
import logging
from typing import Any


class UnifiedLogger:
    """Lightweight wrapper producing structured log records."""

    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(message)s")
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)
        self._logger.setLevel(logging.INFO)

    def _log(self, level: int, event: str, **fields: Any) -> None:
        payload = {"event": event, **fields}
        self._logger.log(level, json.dumps(payload, ensure_ascii=False))

    def info(self, event: str, **fields: Any) -> None:
        self._log(logging.INFO, event, **fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._log(logging.WARNING, event, **fields)

    def error(self, event: str, **fields: Any) -> None:
        self._log(logging.ERROR, event, **fields)


__all__ = ["UnifiedLogger"]
