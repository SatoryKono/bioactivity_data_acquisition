"""Stable, backwards-compatible public API surface for bioetl.

This module centralizes re-exports so that downstream integrations can rely on
import paths that remain stable even if internal modules move.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from bioetl.config import PipelineConfig, load_config
from bioetl.core.http.api_client import UnifiedAPIClient
from bioetl.core.logging import UnifiedLogger
from bioetl.core.types import RunResult
from bioetl.pipelines.base import PipelineBase


BaseApiClient = UnifiedAPIClient


@runtime_checkable
class IParser(Protocol):
    """Lightweight parser contract for backward compatibility."""

    def parse(self, payload: Any) -> Any:  # pragma: no cover - protocol definition
        ...


@runtime_checkable
class INormalizer(Protocol):
    """Lightweight normalizer contract for backward compatibility."""

    def normalize(self, payload: Any) -> Any:  # pragma: no cover - protocol definition
        ...


__all__ = [
    "BaseApiClient",
    "IParser",
    "INormalizer",
    "PipelineBase",
    "PipelineConfig",
    "RunResult",
    "UnifiedLogger",
    "load_config",
]
