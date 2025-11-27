"""Билдер для сервисов артефактов рантайма."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, TYPE_CHECKING

from bioetl.core.pipeline.services import (
    ArtifactPlanner,
    ArtifactRuntimeService,
    default_artifact_runtime_service_factory,
)

if TYPE_CHECKING:  # pragma: no cover
    from bioetl.core.pipeline.runtime import PipelineRuntimeBase


class ArtifactRuntimeBuilderProtocol:
    """Интерфейс построителя сервисов артефактов."""

    def build(self, pipeline: "PipelineRuntimeBase") -> ArtifactRuntimeService:
        """Построить сервис артефактов."""


@dataclass(slots=True)
class ArtifactRuntimeBuilder(ArtifactRuntimeBuilderProtocol):
    """Билдер для сервисов артефактов."""

    factory: Callable[["PipelineRuntimeBase"], ArtifactRuntimeService] | None = None
    artifact_planner: ArtifactPlanner | None = None
    runtime_service: ArtifactRuntimeService | None = None

    def build(self, pipeline: "PipelineRuntimeBase") -> ArtifactRuntimeService:
        """Построить сервис артефактов."""

        if self.runtime_service is not None:
            return self.runtime_service

        factory = (
            self.factory
            or default_artifact_runtime_service_factory(
                artifact_planner=self.artifact_planner
            )
        )
        return factory(pipeline)


__all__ = [
    "ArtifactRuntimeBuilderProtocol",
    "ArtifactRuntimeBuilder",
]
