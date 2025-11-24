"""Compatibility shim for legacy ``application.pipelines.specs.errors`` imports."""

from __future__ import annotations

from application.pipelines.errors import (
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
