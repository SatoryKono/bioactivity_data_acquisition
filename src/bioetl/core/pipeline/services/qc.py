"""QC services for pipeline execution."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, cast

from bioetl.core.io.artifacts import WriteArtifacts
from bioetl.core.pipeline.types import (
    StageContextProtocol,
    StageExecutionOptions,
)
from bioetl.core.runtime.qc import (
    default_qc_runtime_service_factory,
    default_qc_service_factory,
)
from bioetl.qc.executor import QCMetricsExecutor
from bioetl.qc.plan import QCPlan

# Re-export factories
__all__ = [
    "QCExecutorAdapter",
    "QCService",
    "QCOrchestrator",
    "QCRuntimeService",
    "default_qc_runtime_service_factory",
    "default_qc_service_factory",
]


class QCExecutorAdapter:
    """Thin wrapper over :class:`QCMetricsExecutor` with artifact wiring."""

    def __init__(
        self,
        *,
        executor_factory: Callable[[], QCMetricsExecutor] | None = None,
    ) -> None:
        self.executor_factory = executor_factory

    def execute(
        self,
        context: StageContextProtocol,
        plan: QCPlan,
        artifacts: WriteArtifacts | None = None,
    ) -> Path | None:
        """
        Execute QC metrics calculation and save reports.

        Args:
            context: Stage context containing the dataframe.
            plan: QC execution plan.
            artifacts: Optional output artifacts to update.

        Returns:
            Path to the QC metrics JSON file, or None if no metrics produced.
        """
        current_df = context.data_bucket.get()
        if current_df is None:
            return None

        dataset_artifacts = artifacts or context.artifact_store.get()
        any_artifacts = cast(Any, dataset_artifacts)
        dataset_name = (
            any_artifacts.data_path.stem
            if any_artifacts and any_artifacts.data_path
            else "dataset"
        )
        executor_factory = self.executor_factory or QCMetricsExecutor
        executor = executor_factory()
        quality_report, metrics_payload = executor.execute(
            current_df,
            plan,
            dataset_name=dataset_name,
        )
        if quality_report.empty and not metrics_payload:
            return None

        qc_dir = context.output_dir / "qc"
        qc_dir.mkdir(parents=True, exist_ok=True)
        quality_path = qc_dir / f"{dataset_name}_quality_report.csv"
        metrics_path = qc_dir / f"{dataset_name}_qc_metrics.json"
        quality_report.to_csv(quality_path, index=False)
        metrics_path.write_text(json.dumps(metrics_payload, indent=2))
        any_artifacts.quality_report_path = quality_path
        any_artifacts.qc_summary_path = metrics_path
        context.artifact_store.set(any_artifacts)
        return metrics_path


class QCService:
    """Service wrapper around QC execution pipeline."""

    def __init__(
        self,
        adapter: QCExecutorAdapter | None = None,
        *,
        enabled: bool | None = None,
        plan: QCPlan | None = None,
        dry_run: bool | None = None,
        thresholds: Mapping[str, float] | None = None,
    ) -> None:
        self.adapter = adapter or QCExecutorAdapter()
        self.enabled = enabled
        self.plan = plan
        self.dry_run = dry_run
        self.thresholds = thresholds or {}

    def execute(
        self,
        context: StageContextProtocol,
        options: StageExecutionOptions,
    ) -> Path | None:
        """
        Execute QC workflow if enabled.

        Resolves the effective plan and delegates to adapter.
        """
        if self.enabled is False or not options.include_qc_metrics:
            return None
        resolved_plan = self._resolve_plan(context, options)
        if not resolved_plan.enabled:
            return None
        artifacts = context.artifact_store.get()
        return self.adapter.execute(context, resolved_plan, artifacts)

    def _resolve_plan(
        self,
        context: StageContextProtocol,
        options: StageExecutionOptions,
    ) -> QCPlan:
        base_plan = (
            self.plan
            or getattr(context.pipeline, "qc_plan", None)
            or QCPlan.with_default_metrics()
        )
        thresholds = {**base_plan.thresholds, **self.thresholds}
        resolved_dry_run = (
            self.dry_run if self.dry_run is not None else options.dry_run
        )
        plan_updates: dict[str, Mapping[str, float] | bool] = {
            "dry_run": resolved_dry_run,
            "thresholds": thresholds,
        }
        return base_plan.model_copy(update=plan_updates)


@dataclass(slots=True)
class QCOrchestrator:
    """Orchestrates QC execution and error handling."""

    qc_service: QCService

    def run(
        self,
        context: StageContextProtocol,
        options: StageExecutionOptions,
    ) -> tuple[Path | None, str | None]:
        """
        Run the QC process safely.

        Catches any exceptions and returns them as error string.
        """
        try:
            return self.qc_service.execute(context, options), None
        # pylint: disable=broad-exception-caught
        except Exception as exc:  # noqa: BLE001
            return None, str(exc)


@dataclass(slots=True)
class QCRuntimeService:
    """Runtime coordinator for QC execution."""

    qc_service: QCService | None
    qc_orchestrator: QCOrchestrator | None

    def run(
        self, context: StageContextProtocol, options: StageExecutionOptions
    ) -> tuple[Path | None, str | None]:
        """
        Execute QC if orchestrator is available.

        Delegates to QCOrchestrator.
        """
        if self.qc_orchestrator is None:
            return None, None
        return self.qc_orchestrator.run(context, options)
