from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

from .plan import MetricSpec, QCPlan
from .report import build_quality_report

QCMetricCallable = Callable[[pd.DataFrame, MetricSpec], "QCMetricResult"]


@dataclass
class QCMetricResult:
    name: str
    metric_type: str
    value: Any
    threshold: float | None = None
    status: str = "PASS"
    details: Any | None = None
    message: str | None = None

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        if isinstance(self.details, pd.DataFrame):
            payload["details"] = self.details.to_dict(orient="records")
        return payload


class QCFailureException(RuntimeError):
    """Raised when QC metrics violate configured thresholds."""

    def __init__(self, failures: Mapping[str, QCMetricResult]):
        super().__init__("QC thresholds violated")
        self.failures = failures


class MetricRegistry:
    """Registry keeping track of QC metric callables."""

    def __init__(self) -> None:
        self._entries: dict[str, QCMetricCallable] = {}

    def register(self, name: str, func: QCMetricCallable, *, override: bool = False) -> None:
        if not name:
            raise ValueError("metric name must be provided")
        if name in self._entries and not override:
            raise ValueError(f"metric {name!r} already registered")
        self._entries[name] = func

    def get(self, name: str) -> QCMetricCallable:
        try:
            return self._entries[name]
        except KeyError as exc:  # pragma: no cover - defensive
            raise KeyError(f"metric {name!r} is not registered") from exc

    def list_metrics(self) -> tuple[str, ...]:
        return tuple(sorted(self._entries))


DEFAULT_REGISTRY = MetricRegistry()


class QCMetricsExecutor:
    """Executes QC metrics defined by a :class:`QCPlan`."""

    def __init__(self, registry: MetricRegistry | None = None, *, parallel: bool = False, max_workers: int | None = None):
        self.registry = registry or DEFAULT_REGISTRY
        self.parallel = parallel
        self.max_workers = max_workers

    def execute(
        self,
        dataset: pd.DataFrame | Path | str,
        plan: QCPlan,
        *,
        dataset_name: str = "dataset",
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        if not plan.enabled:
            return pd.DataFrame(), {}

        df = self._load_dataset(dataset)

        results = (
            self._execute_dry_run(plan)
            if plan.dry_run
            else self._execute_plan(df, plan)
        )

        quality_report = build_quality_report(df, results, dataset_name=dataset_name)
        metrics_payload = {name: result.to_payload() for name, result in results.items()}

        failures = {
            name: result
            for name, result in results.items()
            if result.status == "FAIL"
        }
        if failures and plan.fail_on_threshold_violation:
            raise QCFailureException(failures)

        return quality_report, metrics_payload

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
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self._run_metric, df.copy(), metric, plan): metric.name
                    for metric in plan.metrics
                }
                for future in as_completed(futures):
                    name = futures[future]
                    results[name] = future.result()
        else:
            for metric in plan.metrics:
                results[metric.name] = self._run_metric(df, metric, plan)
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


def metric_row_count(df: pd.DataFrame, spec: MetricSpec) -> QCMetricResult:
    return QCMetricResult(name=spec.name, metric_type=spec.type, value=int(len(df)))


def metric_null_percentage(df: pd.DataFrame, spec: MetricSpec) -> QCMetricResult:
    if df.empty:
        return QCMetricResult(
            name=spec.name,
            metric_type=spec.type,
            value=0.0,
            details=pd.DataFrame(columns=["column", "null_ratio"]),
        )
    ratios = df.isna().mean()
    details = pd.DataFrame({"column": ratios.index, "null_ratio": ratios.values})
    value = float(ratios.max()) if not ratios.empty else 0.0
    return QCMetricResult(
        name=spec.name,
        metric_type=spec.type,
        value=value,
        details=details.sort_values("column").reset_index(drop=True),
    )


def metric_unique_count(df: pd.DataFrame, spec: MetricSpec) -> QCMetricResult:
    column = spec.params.get("column") if spec.params else None
    if not column or column not in df.columns:
        raise ValueError("unique_count metric requires a valid 'column' parameter")
    unique_count = int(df[column].nunique(dropna=True))
    return QCMetricResult(
        name=spec.name,
        metric_type=spec.type,
        value=unique_count,
    )


def metric_distribution_summary(df: pd.DataFrame, spec: MetricSpec) -> QCMetricResult:
    numeric_df = df.select_dtypes(include=[np.number])
    summary = numeric_df.describe().transpose() if not numeric_df.empty else pd.DataFrame()
    return QCMetricResult(
        name=spec.name,
        metric_type=spec.type,
        value=int(summary.shape[0]),
        details=summary.reset_index().rename(columns={"index": "column"}),
    )


def metric_pka_range(df: pd.DataFrame, spec: MetricSpec) -> QCMetricResult:
    column = spec.params.get("column", "pka")
    minimum = float(spec.params.get("min", 0.0))
    maximum = float(spec.params.get("max", 14.0))
    if column not in df.columns:
        return QCMetricResult(
            name=spec.name,
            metric_type=spec.type,
            value=0.0,
            message=f"column {column!r} not present",
        )
    series = df[column].dropna()
    violations = ((series < minimum) | (series > maximum)).mean() if len(series) else 0.0
    return QCMetricResult(
        name=spec.name,
        metric_type=spec.type,
        value=float(violations),
        details=pd.DataFrame({"violations_ratio": [violations]}),
    )


def metric_pchembl_consistency(df: pd.DataFrame, spec: MetricSpec) -> QCMetricResult:
    primary = spec.params.get("primary", "pchembl_value")
    secondary = spec.params.get("secondary", "standard_value")
    tolerance = float(spec.params.get("tolerance", 0.0))
    if primary not in df.columns or secondary not in df.columns:
        return QCMetricResult(
            name=spec.name,
            metric_type=spec.type,
            value=0.0,
            message="required columns missing",
        )
    series_primary = df[primary]
    series_secondary = df[secondary]
    comparable = series_primary.notna() & series_secondary.notna()
    if not comparable.any():
        return QCMetricResult(name=spec.name, metric_type=spec.type, value=0.0)
    deltas = (series_primary[comparable] - series_secondary[comparable]).abs()
    inconsistent_ratio = float((deltas > tolerance).mean())
    return QCMetricResult(
        name=spec.name,
        metric_type=spec.type,
        value=inconsistent_ratio,
        details=pd.DataFrame({"inconsistent_ratio": [inconsistent_ratio]}),
    )


for name, func in {
    "row_count": metric_row_count,
    "null_percentage": metric_null_percentage,
    "unique_count": metric_unique_count,
    "distribution_summary": metric_distribution_summary,
    "pka_range": metric_pka_range,
    "pchembl_consistency": metric_pchembl_consistency,
}.items():
    DEFAULT_REGISTRY.register(name, func, override=True)


__all__ = [
    "QCMetricCallable",
    "QCMetricResult",
    "QCFailureException",
    "MetricRegistry",
    "DEFAULT_REGISTRY",
    "QCMetricsExecutor",
    "metric_row_count",
    "metric_null_percentage",
    "metric_unique_count",
    "metric_distribution_summary",
    "metric_pka_range",
    "metric_pchembl_consistency",
]
