"""Core pipeline interfaces and errors."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from infrastructure.io import RunArtifacts, WriteArtifacts, WriteResult

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
    "PipelineBase": ("application.pipelines.base", "PipelineBase"),
    "PipelineExtractionMode": (
        "application.pipelines.common",
        "PipelineExtractionMode",
    ),
    "PipelineStageCommand": (
        "application.pipelines.common",
        "PipelineStageCommand",
    ),
    "PipelineStagesProtocol": (
        "application.pipelines.common",
        "PipelineStagesProtocol",
    ),
    "RunResult": ("application.pipelines.common", "RunResult"),
    "StageContext": ("application.pipelines.common", "StageContext"),
    "StageExecutionOptions": (
        "application.pipelines.common",
        "StageExecutionOptions",
    ),
    "StageFactory": ("application.pipelines.common", "StageFactory"),
    "PipelineError": ("application.pipelines.errors", "PipelineError"),
    "PipelineHTTPError": ("application.pipelines.errors", "PipelineHTTPError"),
    "PipelineNetworkError": (
        "application.pipelines.errors",
        "PipelineNetworkError",
    ),
    "PipelineTimeoutError": (
        "application.pipelines.errors",
        "PipelineTimeoutError",
    ),
    "map_client_exc": ("application.pipelines.errors", "map_client_exc"),
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
