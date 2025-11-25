from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

import json

from bioetl.core.pipeline.types import Stage, StageContext, StageExecutionOptions, WriteArtifacts
from bioetl.qc.executor import QCMetricsExecutor
from bioetl.qc.plan import QCPlan


class ArtifactPlanner:
    """Base class responsible for deterministic artifact planning."""

    def plan(
        self, output_dir: Path, pipeline_code: str, run_tag: str | None, mode: str | None
    ) -> tuple[Path, WriteArtifacts]:
        raise NotImplementedError


class DefaultArtifactPlanner(ArtifactPlanner):
    """Simple planner that writes directly into ``output_dir``."""

    def plan(
        self, output_dir: Path, pipeline_code: str, run_tag: str | None, mode: str | None
    ) -> tuple[Path, WriteArtifacts]:
        output_dir.mkdir(parents=True, exist_ok=True)
        artifacts = WriteArtifacts(data_path=output_dir / f"{pipeline_code}.csv")
        return output_dir, artifacts


class QCExecutorAdapter:
    """Обвязка над :class:`QCMetricsExecutor` с обработкой артефактов."""

    def __init__(
        self,
        *,
        executor_factory: Callable[[], QCMetricsExecutor] | None = None,
        qc_plan: QCPlan | None = None,
    ) -> None:
        self.executor_factory = executor_factory
        self.qc_plan = qc_plan

    def execute(
        self, context: StageContext, options: StageExecutionOptions, artifacts: WriteArtifacts
    ) -> Path | None:
        if context.current_df is None or not options.include_qc_metrics:
            return None

        plan = self.qc_plan or getattr(context.pipeline, "qc_plan", None) or QCPlan.with_default_metrics()
        if options.dry_run:
            plan = plan.model_copy(update={"dry_run": True})

        dataset_name = artifacts.data_path.stem if artifacts and artifacts.data_path else "dataset"
        executor_factory = self.executor_factory or QCMetricsExecutor
        executor = executor_factory()
        quality_report, metrics_payload = executor.execute(
            context.current_df, plan, dataset_name=dataset_name
        )
        if quality_report.empty and not metrics_payload:
            return None

        qc_dir = context.output_dir / "qc"
        qc_dir.mkdir(parents=True, exist_ok=True)
        quality_path = qc_dir / f"{dataset_name}_quality_report.csv"
        metrics_path = qc_dir / f"{dataset_name}_qc_metrics.json"
        quality_report.to_csv(quality_path, index=False)
        metrics_path.write_text(json.dumps(metrics_payload, indent=2))
        if artifacts:
            artifacts.quality_report_path = quality_path
            artifacts.qc_summary_path = metrics_path
            context.artifacts = artifacts
        return metrics_path


class QCService:
    """Service wrapper around QC execution pipeline."""

    def __init__(self, adapter: QCExecutorAdapter | None = None) -> None:
        self.adapter = adapter or QCExecutorAdapter()

    def execute(self, context: StageContext, options: StageExecutionOptions) -> Path | None:
        artifacts = context.artifacts or WriteArtifacts()
        return self.adapter.execute(context, options, artifacts)


@dataclass(slots=True)
class MetadataService:
    """Service delegating metadata building to injected builder."""

    builder: Any

    def build(
        self,
        context: StageContext,
        stage_plan: Iterable[Stage],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
    ) -> dict[str, Any]:
        return self.builder.build(context, stage_plan, durations, run_tag, mode)


__all__ = [
    "ArtifactPlanner",
    "DefaultArtifactPlanner",
    "QCExecutorAdapter",
    "QCService",
    "MetadataService",
]
