from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from bioetl.core.pipeline import factories
from bioetl.core.pipeline.services import (
    OrchestrationService,
    QCRuntimeService,
    StagePlanExecutor,
    default_artifact_service_factory,
)
from bioetl.core.runtime import MetadataCoordinator, QCCoordinator


def test_default_artifact_runtime_service_factory_uses_builder() -> None:
    runtime = object()
    artifact_runtime_service = MagicMock()
    builder = MagicMock()
    builder.build.return_value = artifact_runtime_service

    factory = factories.default_artifact_runtime_service_factory(
        artifact_runtime_builder=builder,
    )

    result = factory(runtime)

    assert result is artifact_runtime_service
    builder.build.assert_called_once_with(runtime)


def test_default_qc_runtime_service_factory_builds_coordinator() -> None:
    stage_plan_executor = StagePlanExecutor()
    runtime_service = QCRuntimeService(qc_service=None, qc_orchestrator=None)
    coordinator = QCCoordinator(
        qc_runtime_service=runtime_service,
        stage_plan_executor=stage_plan_executor,
    )
    builder = MagicMock()
    builder.build.return_value = coordinator

    factory = factories.default_qc_runtime_service_factory(
        stage_plan_executor=stage_plan_executor,
        qc_runtime_builder=builder,
    )

    service, built_coordinator = factory(object())

    assert service is runtime_service
    assert built_coordinator is coordinator
    builder.build.assert_called_once_with(stage_plan_executor)


def test_default_metadata_runtime_service_factory_returns_coordinator() -> None:
    runtime_service = SimpleNamespace(
        metadata_service=MagicMock(),
        git_commit="git-hash",
        config_hash="config-hash",
    )
    coordinator = MetadataCoordinator(
        metadata_runtime_service=runtime_service,
        logs_directory_resolver=lambda path: path,
    )
    builder = MagicMock()
    builder.build.return_value = coordinator

    factory = factories.default_metadata_runtime_service_factory(
        config={},
        pipeline_code="pipeline",
        logs_directory_resolver=lambda path: path,
        metadata_runtime_builder=builder,
    )

    service, built_coordinator = factory(object())

    assert service is runtime_service
    assert built_coordinator is coordinator
    builder.build.assert_called_once_with()


def test_default_orchestration_service_factory_uses_coordinator() -> None:
    stage_plan_executor = StagePlanExecutor()
    artifact_service = default_artifact_service_factory()

    factory = factories.default_orchestration_service_factory(
        stage_plan_executor=stage_plan_executor,
        artifact_service=artifact_service,
    )

    orchestration_service = factory(object())

    assert isinstance(orchestration_service, OrchestrationService)
    assert orchestration_service.stage_plan_executor is stage_plan_executor
    assert orchestration_service.artifact_service is artifact_service
