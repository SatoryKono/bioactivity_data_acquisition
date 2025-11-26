"""Lightweight pipeline definition helpers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Mapping, Sequence

if TYPE_CHECKING:
    from bioetl.core.pipeline.factory import StageFactory


@dataclass(slots=True)
class PipelineDefinition:
    """Declarative wrapper for pipeline factories.

    The definition bundles together the factory that builds a runtime pipeline
    instance along with optional stage/QC helpers. The ``validate`` method is
    intentionally strict to catch configuration errors before any pipeline code
    executes.
    """

    name: str
    runtime_factory: Callable[[], Any]
    stages: Sequence[str] | None = None
    stage_factory: type[StageFactory] | None = None
    qc_registry: Mapping[str, Any] | None = None

    def validate(self) -> None:
        if not self.name:
            msg = "pipeline name must be provided"
            raise ValueError(msg)
        if not callable(self.runtime_factory):
            msg = "runtime_factory must be callable"
            raise ValueError(msg)
        if self.stages is not None:
            if any(not stage for stage in self.stages):
                msg = "stages must not contain empty names"
                raise ValueError(msg)
            if len(set(self.stages)) != len(tuple(self.stages)):
                msg = "stages must be unique"
                raise ValueError(msg)
        if self.stage_factory is not None:
            from bioetl.core.pipeline.factory import StageFactory as _StageFactory

            if not issubclass(self.stage_factory, _StageFactory):
                msg = "stage_factory must be a StageFactory subclass"
                raise ValueError(msg)


__all__ = ["PipelineDefinition"]
