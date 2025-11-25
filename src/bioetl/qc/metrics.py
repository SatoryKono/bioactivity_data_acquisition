from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Callable, Mapping

import numpy as np
import pandas as pd

from .plan import MetricSpec
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
    "metric_row_count",
    "metric_null_percentage",
    "metric_unique_count",
    "metric_distribution_summary",
    "metric_pka_range",
    "metric_pchembl_consistency",
]
