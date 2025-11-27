"""Factory helpers for pipeline runtime services."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Tuple

from bioetl.core.pipeline.artifact_runtime_builder import (
    ArtifactRuntimeBuilder,
    ArtifactRuntimeBuilderProtocol,
)
from bioetl.core.pipeline.metadata_runtime_builder import MetadataRuntimeBuilder
from bioetl.core.pipeline.qc_runtime_builder import QCRuntimeBuilder
from bioetl.core.pipeline.services import (
    ArtifactRuntimeService,
    MetadataRuntimeService,
    OrchestrationService,
    StagePlanExecutor,
)
from bioetl.core.runtime import MetadataCoordinator, OrchestrationCoordinator, QCCoordinator


def default_artifact_runtime_service_factory(
    *,
    artifact_runtime_builder: ArtifactRuntimeBuilderProtocol | None = None,
    artifact_runtime_service: ArtifactRuntimeService | None = None,
) -> Callable[[Any], ArtifactRuntimeService]:
    """Build an artifact runtime service factory bound to the provided builder."""

    builder = artifact_runtime_builder or ArtifactRuntimeBuilder()

    def _factory(runtime: Any) -> ArtifactRuntimeService:
        if artifact_runtime_service is not None:
            return artifact_runtime_service
        return builder.build(runtime)

    return _factory


def default_qc_runtime_service_factory(
    *,
    stage_plan_executor: StagePlanExecutor | None = None,
    qc_runtime_builder: QCRuntimeBuilder | None = None,
) -> Callable[[Any], Tuple[Any, QCCoordinator]]:
    """Build a QC runtime service factory returning service and coordinator."""

    builder = qc_runtime_builder or QCRuntimeBuilder()

    def _factory(_: Any) -> Tuple[Any, QCCoordinator]:
        coordinator = builder.build(stage_plan_executor)
        return coordinator.qc_runtime_service, coordinator

    return _factory


def default_metadata_runtime_service_factory(
    *,
    config: Any,
    pipeline_code: str,
    logs_directory_resolver: Callable[[Path], Path],
    metadata_runtime_builder: MetadataRuntimeBuilder | None = None,
) -> Callable[[Any], Tuple[MetadataRuntimeService, MetadataCoordinator]]:
    """Build a metadata runtime factory returning service and coordinator."""

    builder = metadata_runtime_builder or MetadataRuntimeBuilder(
        config=config,
        pipeline_code=pipeline_code,
        logs_directory_resolver=logs_directory_resolver,
    )

    def _factory(_: Any) -> Tuple[MetadataRuntimeService, MetadataCoordinator]:
        coordinator = builder.build()
        return coordinator.metadata_runtime_service, coordinator

    return _factory


def default_orchestration_service_factory(
    *,
    stage_plan_executor: StagePlanExecutor,
    artifact_service: Any,
    orchestration_service_factory: Callable[[OrchestrationCoordinator], OrchestrationService]
    | None = None,
) -> Callable[[Any], OrchestrationService]:
    """Build an orchestration service factory bound to coordinator."""

    from bioetl.core.pipeline.services import (
        default_orchestration_service_factory as _default_orchestration_service_factory,
    )

    def _factory(_: Any) -> OrchestrationService:
        coordinator = OrchestrationCoordinator(
            stage_plan_executor=stage_plan_executor,
            artifact_service=artifact_service,
        )
        orchestration_factory = orchestration_service_factory or _default_orchestration_service_factory(
            stage_plan_executor=stage_plan_executor,
            artifact_service=artifact_service,
        )
        return orchestration_factory(coordinator)

    return _factory


__all__ = [
    "default_artifact_runtime_service_factory",
    "default_qc_runtime_service_factory",
    "default_metadata_runtime_service_factory",
    "default_orchestration_service_factory",
]
