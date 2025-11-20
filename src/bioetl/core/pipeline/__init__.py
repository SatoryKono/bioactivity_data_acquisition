"""Core pipeline interfaces and errors."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from bioetl.core.io import RunArtifacts, WriteArtifacts, WriteResult

if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    from .base import (
        PipelineBase,
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

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "PipelineBase": ("bioetl.core.pipeline.base", "PipelineBase"),
    "PipelineExtractionMode": ("bioetl.core.pipeline.common", "PipelineExtractionMode"),
    "PipelineStageCommand": ("bioetl.core.pipeline.common", "PipelineStageCommand"),
    "PipelineStagesProtocol": ("bioetl.core.pipeline.common", "PipelineStagesProtocol"),
    "RunResult": ("bioetl.core.pipeline.common", "RunResult"),
    "StageContext": ("bioetl.core.pipeline.common", "StageContext"),
    "StageExecutionOptions": ("bioetl.core.pipeline.common", "StageExecutionOptions"),
    "StageFactory": ("bioetl.core.pipeline.common", "StageFactory"),
    "PipelineError": ("bioetl.core.pipeline.errors", "PipelineError"),
    "PipelineHTTPError": ("bioetl.core.pipeline.errors", "PipelineHTTPError"),
    "PipelineNetworkError": ("bioetl.core.pipeline.errors", "PipelineNetworkError"),
    "PipelineTimeoutError": ("bioetl.core.pipeline.errors", "PipelineTimeoutError"),
    "map_client_exc": ("bioetl.core.pipeline.errors", "map_client_exc"),
}


def __getattr__(name: str) -> Any:
    if name not in _LAZY_EXPORTS:
        raise AttributeError(name)
    module_name, attr_name = _LAZY_EXPORTS[name]
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value

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
