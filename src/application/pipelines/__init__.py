"""Core pipeline interfaces and errors."""

from __future__ import annotations

from infrastructure.io import RunArtifacts, WriteArtifacts, WriteResult

from .base import PipelineBase
from .common import (
    PipelineExtractionMode,
    PipelineStageCommand,
    PipelineStagesProtocol,
    RunResult,
    StageContext,
    StageExecutionOptions,
    StageFactory,
)
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
    "PipelineStageCommand",
    "PipelineStagesProtocol",
    "PipelineError",
    "PipelineHTTPError",
    "PipelineNetworkError",
    "PipelineTimeoutError",
    "StageContext",
    "StageExecutionOptions",
    "StageFactory",
    "RunArtifacts",
    "RunResult",
    "WriteArtifacts",
    "WriteResult",
    "map_client_exc",
]
