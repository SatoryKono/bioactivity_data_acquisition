"""Core pipeline interfaces and errors."""

from bioetl.core.io import RunArtifacts, WriteArtifacts, WriteResult

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
    "RunArtifacts",
    "RunResult",
    "WriteArtifacts",
    "WriteResult",
    "map_client_exc",
]

