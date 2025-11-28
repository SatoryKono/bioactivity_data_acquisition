"""Lightweight protocol and placeholder types for pipeline runtime."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class PipelineBaseProtocol(Protocol):
    """Minimal pipeline interface placeholder."""


class StageProtocol(Protocol):
    """Represents an executable pipeline stage."""


class StageContextProtocol(Protocol):
    """Carries contextual information for a stage execution."""


class StageRuntimeContext(Protocol):
    """Runtime metadata for a stage."""


@dataclass(slots=True)
class StageExecutionOptions:
    """Execution options used across pipeline components."""

    dry_run: bool = False


class StageDescriptor(Protocol):
    """Describes a stage for orchestration purposes."""


class StageCommand(Protocol):
    """Callable interface for executing a stage."""


class ArtifactStore(Protocol):
    """Abstract store for pipeline artifacts."""


class DataBucket(Protocol):
    """Represents a logical grouping of dataset artifacts."""


class DefaultArtifactContext(Protocol):
    """Context for artifact materialization."""


class DefaultDomainContext(Protocol):
    """Domain-level contextual information."""


class DefaultExecutionContext(Protocol):
    """Execution-level contextual information."""


class DefaultInfrastructureContext(Protocol):
    """Infrastructure-level contextual information."""


class RunResult(Protocol):
    """Aggregate result for a pipeline run."""


class RunState(Protocol):
    """Represents lifecycle state for a pipeline run."""


class WriteArtifacts(Protocol):
    """Artifacts produced by write services."""


WriteResult = Any


__all__ = [
    "ArtifactStore",
    "DataBucket",
    "DefaultArtifactContext",
    "DefaultDomainContext",
    "DefaultExecutionContext",
    "DefaultInfrastructureContext",
    "PipelineBaseProtocol",
    "RunResult",
    "RunState",
    "StageCommand",
    "StageContextProtocol",
    "StageDescriptor",
    "StageExecutionOptions",
    "StageProtocol",
    "StageRuntimeContext",
    "WriteArtifacts",
    "WriteResult",
]

