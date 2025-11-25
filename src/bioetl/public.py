"""Stable, backwards-compatible public API surface for bioetl.

This module centralizes re-exports so that downstream integrations can rely on
import paths that remain stable even if internal modules move.
"""

from __future__ import annotations

from bioetl.config import PipelineConfig, load_config
from bioetl.core.http.api_client import UnifiedAPIClient
from bioetl.core.logging import UnifiedLogger
from bioetl.core.types import RunResult
from bioetl.pipelines.base import PipelineBase


BaseApiClient = UnifiedAPIClient

__all__ = [
    "BaseApiClient",
    "PipelineBase",
    "PipelineConfig",
    "RunResult",
    "UnifiedLogger",
    "load_config",
]
