"""Compatibility shim for legacy ``bioetl.pipelines.errors`` imports."""

from __future__ import annotations

from bioetl.core.pipeline.errors import (
    PipelineError,
    PipelineHTTPError,
    PipelineNetworkError,
    PipelineTimeoutError,
    map_client_exc,
)

__all__ = [
    "PipelineError",
    "PipelineHTTPError",
    "PipelineNetworkError",
    "PipelineTimeoutError",
    "map_client_exc",
]

