"""Core pipeline interfaces and errors."""

from .base import PipelineBase, RunResult
from .errors import (
    PipelineError,
    PipelineHTTPError,
    PipelineNetworkError,
    PipelineTimeoutError,
    map_client_exc,
)

__all__ = [
    "PipelineBase",
    "PipelineError",
    "PipelineHTTPError",
    "PipelineNetworkError",
    "PipelineTimeoutError",
    "RunResult",
    "map_client_exc",
]

