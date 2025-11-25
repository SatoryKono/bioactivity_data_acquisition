from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping

from bioetl.core.pipeline.types import Stage, StageContext, StageExecutionOptions


@dataclass(frozen=True, slots=True)
class PipelineDefinition:
    """Immutable pipeline description decoupled from runtime concerns."""

    stages: tuple[Stage, ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)
    version: str = "1.0.0"

    def validate(self) -> None:
        """Ensure the definition is structurally sound without runtime deps."""

        if not self.version:
            raise ValueError("PipelineDefinition.version must be a non-empty string")

        seen: set[str] = set()
        for stage in self.stages:
            stage.validate()
            if stage.name in seen:
                raise ValueError(f"Duplicate stage name detected: {stage.name}")
            seen.add(stage.name)

    def stage_names(self) -> tuple[str, ...]:
        return tuple(stage.name for stage in self.stages)


def build_pipeline_definition(
    stages: Iterable[tuple[str, Callable[[StageContext, StageExecutionOptions], Any]]],
    *,
    metadata: Mapping[str, Any] | None = None,
    version: str = "1.0.0",
) -> PipelineDefinition:
    """Helper to construct a :class:`PipelineDefinition` from raw handlers."""

    stage_objects = tuple(Stage(name, handler) for name, handler in stages)
    definition = PipelineDefinition(stage_objects, metadata=metadata or {}, version=version)
    definition.validate()
    return definition


__all__ = ["PipelineDefinition", "build_pipeline_definition"]
