"""Билдеры для сервисов QC рантайма."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping

from bioetl.core.pipeline.services import QCService, StagePlanExecutor
from bioetl.core.runtime.qc import (
    QCCoordinator,
    QCRuntimeBuilderProtocol,
    default_qc_runtime_service_factory,
)
from bioetl.qc.executor import QCMetricsExecutor
from bioetl.qc.plan import QCPlan


@dataclass(slots=True)
class QCRuntimeBuilder(QCRuntimeBuilderProtocol):
    """Билдер для сервисов QC."""

    qc_runtime_service_factory: Callable[[QCCoordinator], Any] | None = None
    qc_service_factory: Callable[[QCCoordinator], QCService] | None = None
    qc_service: QCService | None = None
    qc_executor_factory: Callable[[], QCMetricsExecutor] | None = None
    qc_plan: QCPlan | None = None
    qc_thresholds: Mapping[str, float] | None = None
    qc_dry_run: bool | None = None
    qc_enabled: bool | None = None
    qc_runtime_service: Any | None = None

    def build(self, stage_plan_executor: StagePlanExecutor | None) -> QCCoordinator:
        """Построить координатор QC."""

        if self.qc_runtime_service is not None:
            return QCCoordinator(
                qc_runtime_service=self.qc_runtime_service,
                stage_plan_executor=stage_plan_executor,
            )

        factory = self.qc_runtime_service_factory or default_qc_runtime_service_factory(
            qc_service_factory=self.qc_service_factory,
            qc_service=self.qc_service,
            qc_executor_factory=self.qc_executor_factory,
            qc_plan=self.qc_plan,
            qc_thresholds=dict(self.qc_thresholds) if self.qc_thresholds else None,
            qc_dry_run=self.qc_dry_run,
            qc_enabled=self.qc_enabled,
        )
        return QCCoordinator.from_factory(
            qc_runtime_service_factory=factory,
            stage_plan_executor=stage_plan_executor,
        )


__all__ = ["QCRuntimeBuilder"]
