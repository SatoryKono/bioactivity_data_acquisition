"""Core pipeline interfaces and errors."""

from bioetl.core.io import RunArtifacts, WriteArtifacts, WriteResult

from .base import PipelineBase, PipelineExtractionMode, PipelineStagesProtocol, RunResult
from .errors import (
    PipelineError,
    PipelineHTTPError,
    PipelineNetworkError,
    PipelineTimeoutError,
    map_client_exc,
)

__all__ = [
    "PipelineBase",
    "PipelineExtractionMode",
    "PipelineStagesProtocol",
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
