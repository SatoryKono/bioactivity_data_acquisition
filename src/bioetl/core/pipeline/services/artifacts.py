"""Artifact services for pipeline execution."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, cast

from bioetl.core.pipeline.types import PipelineBaseProtocol, WriteArtifacts


class ArtifactPlanner:
    """Base class responsible for deterministic artifact planning."""

    def plan(
        self,
        output_dir: Path,
        pipeline_code: str,
        run_tag: str | None,
        mode: str | None,
    ) -> tuple[Path, WriteArtifacts]:
        """
        Abstract method to plan artifacts.

        Must be implemented by subclasses.
        """
        raise NotImplementedError


class DefaultArtifactPlanner(ArtifactPlanner):
    """Simple planner that writes directly into ``output_dir``."""

    def plan(
        self,
        output_dir: Path,
        pipeline_code: str,
        run_tag: str | None,
        mode: str | None,
    ) -> tuple[Path, WriteArtifacts]:
        """Plan artifacts by appending pipeline code to output directory."""
        output_dir.mkdir(parents=True, exist_ok=True)
        write_artifacts_cls = cast(Any, WriteArtifacts)
        artifacts = cast(WriteArtifacts, write_artifacts_cls())
        any_artifacts = cast(Any, artifacts)
        any_artifacts.data_path = output_dir / f"{pipeline_code}.csv"
        return output_dir, artifacts


@dataclass(slots=True)
class ArtifactService:
    """Service responsible for deterministic artifact planning."""

    artifact_planner: ArtifactPlanner

    def plan_run_artifacts(
        self,
        output_dir: Path,
        pipeline_code: str,
        run_tag: str | None,
        mode: str | None,
    ) -> tuple[Path, WriteArtifacts]:
        """
        Plan output paths and artifacts for a pipeline run.

        Args:
            output_dir: Base output directory.
            pipeline_code: Code of the pipeline.
            run_tag: Optional run tag.
            mode: Execution mode.

        Returns:
            Tuple of (resolved_output_path, WriteArtifacts).
        """
        return self.artifact_planner.plan(
            output_dir, pipeline_code, run_tag, mode
        )


@dataclass(slots=True)
class ArtifactRuntimeService:
    """Pipeline-level artifact planning helper."""

    artifact_planner: ArtifactPlanner
    artifact_service: ArtifactService

    def plan_run_artifacts(
        self,
        output_dir: Path,
        pipeline_code: str,
        run_tag: str | None,
        mode: str | None,
    ) -> tuple[Path, WriteArtifacts]:
        """Plan artifacts for the run (delegates to artifact service)."""
        return self.artifact_service.plan_run_artifacts(
            output_dir, pipeline_code, run_tag, mode
        )


def default_artifact_planner_factory() -> ArtifactPlanner:
    """Create a default artifact planner."""
    return DefaultArtifactPlanner()


def default_artifact_service_factory(
    artifact_planner: ArtifactPlanner | None = None,
) -> ArtifactService:
    """Create a default artifact service."""
    return ArtifactService(artifact_planner or DefaultArtifactPlanner())


def default_artifact_runtime_service_factory(
    artifact_planner: ArtifactPlanner | None = None,
) -> Callable[[PipelineBaseProtocol], ArtifactRuntimeService]:
    """Create a factory for the default artifact runtime service."""
    def _factory(_: PipelineBaseProtocol) -> ArtifactRuntimeService:
        planner = artifact_planner or default_artifact_planner_factory()
        return ArtifactRuntimeService(
            artifact_planner=planner,
            artifact_service=default_artifact_service_factory(planner),
        )

    return _factory
