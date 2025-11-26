"""Координация сервисов контроля качества."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from bioetl.core.pipeline.services import (
        QCRuntimeService,
        StagePlanExecutor,
    )

from bioetl.core.pipeline.types import (
    StageContextProtocol,
    StageExecutionOptions,
)
from bioetl.qc.executor import QCMetricsExecutor
from bioetl.qc.plan import QCPlan


class QCOrchestratorProtocol(Protocol):
    """Минимальный интерфейс для оркестрации QC."""

    qc_service: Any

    def run(
        self, context: StageContextProtocol, options: StageExecutionOptions
    ) -> tuple[Path | None, str | None]:
        ...


class QCRuntimeProtocol(Protocol):
    """Тонкий протокол для сервисов выполнения QC."""

    qc_service: Any | None
    qc_orchestrator: QCOrchestratorProtocol | None

    def run(
        self, context: StageContextProtocol, options: StageExecutionOptions
    ) -> tuple[Path | None, str | None]:
        ...


class QCRuntimeBuilderProtocol(Protocol):
    """Протокол билдера для QCRuntimeService."""

    def build(self, coordinator: "QCCoordinator") -> "QCRuntimeService":
        ...


class QCCoordinator:
    """Координатор, отвечающий за QC-оркестрацию и привязку executor."""

    def __init__(
        self,
        *,
        qc_runtime_service: QCRuntimeProtocol,
        stage_plan_executor: StagePlanExecutor | None = None,
    ) -> None:
        if stage_plan_executor is None:
            from bioetl.core.pipeline.services import StagePlanExecutor

            stage_plan_executor = StagePlanExecutor()

        self.qc_runtime_service = qc_runtime_service
        self.stage_plan_executor = self._attach_qc_orchestrator(
            stage_plan_executor
        )

    @classmethod
    def from_factory(
        cls,
        *,
        qc_runtime_service_factory: Callable[
            ["QCCoordinator"], "QCRuntimeService"
        ],
        stage_plan_executor: StagePlanExecutor | None,
    ) -> "QCCoordinator":
        placeholder = cls.__new__(cls)
        runtime_service = qc_runtime_service_factory(placeholder)
        cls.__init__(
            placeholder,
            qc_runtime_service=runtime_service,
            stage_plan_executor=stage_plan_executor,
        )
        return placeholder

    @classmethod
    def from_builder(
        cls,
        *,
        builder: QCRuntimeBuilderProtocol,
        stage_plan_executor: StagePlanExecutor | None,
    ) -> "QCCoordinator":
        placeholder = cls.__new__(cls)
        runtime_service = builder.build(placeholder)
        cls.__init__(
            placeholder,
            qc_runtime_service=runtime_service,
            stage_plan_executor=stage_plan_executor,
        )
        return placeholder

    def _attach_qc_orchestrator(
        self, stage_plan_executor: StagePlanExecutor
    ) -> StagePlanExecutor:
        stage_plan_executor.qc_orchestrator = self.qc_orchestrator
        return stage_plan_executor

    @property
    def qc_service(self) -> Any | None:
        return getattr(self.qc_runtime_service, "qc_service", None)

    @qc_service.setter
    def qc_service(self, value: Any | None) -> None:
        if hasattr(self.qc_runtime_service, "qc_service"):
            self.qc_runtime_service.qc_service = value
        if self.qc_runtime_service and hasattr(
            self.qc_runtime_service, "qc_orchestrator"
        ):
            orchestrator = getattr(
                self.qc_runtime_service, "qc_orchestrator", None
            )
            if orchestrator is not None:
                orchestrator.qc_service = value
            elif value is not None:
                from bioetl.core.pipeline.services import QCOrchestrator

                self.qc_runtime_service.qc_orchestrator = QCOrchestrator(value)

    @property
    def qc_orchestrator(self) -> QCOrchestratorProtocol | None:
        return getattr(self.qc_runtime_service, "qc_orchestrator", None)


def default_qc_runtime_service_factory(
    *,
    qc_service_factory: Callable[["QCCoordinator"], Any] | None = None,
    qc_service: Any | None = None,
    qc_executor_factory: Callable[[], QCMetricsExecutor] | None = None,
    qc_plan: QCPlan | None = None,
    qc_thresholds: dict[str, float] | None = None,
    qc_dry_run: bool | None = None,
    qc_enabled: bool | None = None,
) -> Callable[["QCCoordinator"], QCRuntimeService]:
    from bioetl.core.pipeline.services import QCOrchestrator, QCRuntimeService

    def _factory(coordinator: QCCoordinator) -> QCRuntimeService:
        if qc_service is not None:
            resolved_service = qc_service
        else:
            adapter_factory = qc_service_factory or default_qc_service_factory(
                qc_plan=qc_plan,
                executor_factory=qc_executor_factory,
                qc_thresholds=qc_thresholds,
                qc_dry_run=qc_dry_run,
                qc_enabled=qc_enabled,
            )
            resolved_service = adapter_factory(coordinator)
        orchestrator = (
            QCOrchestrator(resolved_service) if resolved_service else None
        )
        return QCRuntimeService(resolved_service, orchestrator)

    return _factory


def default_qc_service_factory(
    *,
    qc_plan: QCPlan | None = None,
    executor_factory: Callable[[], QCMetricsExecutor] | None = None,
    qc_thresholds: dict[str, float] | None = None,
    qc_dry_run: bool | None = None,
    qc_enabled: bool | None = None,
) -> Callable[["QCCoordinator"], Any]:
    from bioetl.core.pipeline.services import QCExecutorAdapter, QCService

    def _factory(_: QCCoordinator) -> Any:
        return QCService(
            QCExecutorAdapter(executor_factory=executor_factory),
            enabled=qc_enabled,
            plan=qc_plan,
            dry_run=qc_dry_run,
            thresholds=qc_thresholds,
        )

    return _factory


__all__ = [
    "QCCoordinator",
    "QCOrchestratorProtocol",
    "QCRuntimeProtocol",
    "QCRuntimeBuilderProtocol",
    "default_qc_runtime_service_factory",
    "default_qc_service_factory",
]
