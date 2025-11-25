from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from bioetl.qc.metrics import (
    DEFAULT_REGISTRY,
    QCFailureException,
    QCMetricResult,
    MetricRegistry,
)
from bioetl.qc.plan import MetricSpec, QCPlan
from bioetl.qc.report import build_quality_report


class QCMetricsExecutor:
    """Executes QC metrics defined by a :class:`QCPlan`."""

    def __init__(
        self,
        registry: MetricRegistry | None = None,
        *,
        parallel: bool = False,
        max_workers: int | None = None,
    ) -> None:
        self.registry = registry or DEFAULT_REGISTRY
        self.parallel = parallel
        self.max_workers = max_workers

    def execute(
        self,
        dataset: pd.DataFrame | Path | str,
        plan: QCPlan | None = None,
        *,
        dataset_name: str = "dataset",
        dry_run: bool | None = None,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        active_plan = self._resolve_plan(plan, dry_run)
        if not active_plan.enabled:
            return pd.DataFrame(), {}
        if not active_plan.metrics:
            return pd.DataFrame(), {}

        df = self._load_dataset(dataset)

        results = (
            self._execute_dry_run(active_plan)
            if active_plan.dry_run
            else self._execute_plan(df, active_plan)
        )

        quality_report = build_quality_report(df, results, dataset_name=dataset_name)
        metrics_payload = {name: result.to_payload() for name, result in results.items()}

        failures = {
            name: result
            for name, result in results.items()
            if result.status == "FAIL"
        }
        if failures and active_plan.fail_on_threshold_violation:
            raise QCFailureException(failures)

        return quality_report, metrics_payload

    def _resolve_plan(self, plan: QCPlan | None, dry_run: bool | None) -> QCPlan:
        resolved = plan or QCPlan.with_default_metrics()
        if dry_run is None:
            return resolved
        update: Mapping[str, bool] = {"dry_run": dry_run}
        return resolved.model_copy(update=update)

    def _execute_dry_run(self, plan: QCPlan) -> dict[str, QCMetricResult]:
        return {
            metric.name: QCMetricResult(
                name=metric.name,
                metric_type=metric.type,
                value=None,
                status="SKIP",
                message="dry-run enabled",
            )
            for metric in plan.metrics
        }

    def _execute_plan(self, df: pd.DataFrame, plan: QCPlan) -> dict[str, QCMetricResult]:
        results: dict[str, QCMetricResult] = {}
        if self.parallel and len(plan.metrics) > 1:
            results.update(self._execute_parallel(df, plan))
        else:
            for metric in plan.metrics:
                results[metric.name] = self._run_metric(df, metric, plan)
        return results

    def _execute_parallel(self, df: pd.DataFrame, plan: QCPlan) -> dict[str, QCMetricResult]:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        results: dict[str, QCMetricResult] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._run_metric, df.copy(), metric, plan): metric.name
                for metric in plan.metrics
            }
            for future in as_completed(futures):
                name = futures[future]
                results[name] = future.result()
        return results

    def _run_metric(self, df: pd.DataFrame, metric: MetricSpec, plan: QCPlan) -> QCMetricResult:
        func_name = metric.executor or metric.type
        func = self.registry.get(func_name)
        result = func(df, metric)
        threshold = plan.thresholds.get(metric.name) or plan.thresholds.get(metric.type)
        result.threshold = threshold
        if threshold is not None and isinstance(result.value, (int, float, np.integer, np.floating)):
            result.status = "PASS" if float(result.value) <= float(threshold) else "FAIL"
            if result.status == "FAIL":
                result.message = result.message or (
                    f"value {result.value} is above threshold {threshold}"
                )
        return result

    def _load_dataset(self, dataset: pd.DataFrame | Path | str) -> pd.DataFrame:
        if isinstance(dataset, pd.DataFrame):
            return dataset
        path = Path(dataset)
        if path.suffix.lower() == ".parquet":
            return pd.read_parquet(path)
        return pd.read_csv(path)


__all__ = ["QCMetricsExecutor"]
