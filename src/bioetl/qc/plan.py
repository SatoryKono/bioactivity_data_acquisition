"""Declarative QC plan, registry, and execution helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from typing import Callable, Mapping, MutableMapping, Sequence

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from bioetl.qc.metrics import (
    CategoricalDistribution,
    CorrelationMatrix,
    DuplicateStats,
    MissingnessStats,
    OutlierStatistic,
    compute_correlation_matrix,
    compute_duplicate_stats,
    compute_missingness,
    detect_iqr_outliers,
)

class QCPlan(BaseModel):
    """Declarative description of QC steps to execute for a pipeline."""

    duplicates: bool = Field(default=True)
    missingness: bool = Field(default=True)
    units_distribution: bool = Field(default=True)
    relation_distribution: bool = Field(default=True)
    correlation: bool = Field(default=True)
    outliers: bool = Field(default=True)
    outlier_iqr_multiplier: float = Field(default=1.5, ge=0.1, le=100.0)
    outlier_min_count: int = Field(default=1, ge=0)
    custom_metrics: tuple[str, ...] = Field(default_factory=tuple)

    model_config = ConfigDict(frozen=True)

    @model_validator(mode="after")
    def _ensure_unique_custom_metrics(self) -> "QCPlan":
        seen: set[str] = set()
        for name in self.custom_metrics:
            if name in seen:
                msg = f"duplicate custom metric name: {name}"
                raise ValueError(msg)
            seen.add(name)
        return self


QC_PLAN_DEFAULT = QCPlan()


@dataclass(frozen=True)
class QCMetricResult:
    """Structured payload returned by custom QC metrics."""

    name: str
    payload: object
    section: str = "custom"
    rows: tuple[dict[str, object], ...] = ()
    metadata: Mapping[str, object] | None = None


@dataclass(frozen=True)
class QCMetricsBundle:
    """Aggregated results of executing a :class:`QCPlan`."""

    duplicates: DuplicateStats | None = None
    missingness: MissingnessStats | None = None
    units_distribution: CategoricalDistribution | None = None
    relation_distribution: CategoricalDistribution | None = None
    correlation: CorrelationMatrix | None = None
    outliers: tuple[OutlierStatistic, ...] = ()
    custom: Mapping[str, QCMetricResult] = field(default_factory=dict)
    business_key_fields: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class QCExecutionContext:
    """Runtime context passed to custom metrics."""

    plan: QCPlan
    business_key_fields: tuple[str, ...]
    bundle: QCMetricsBundle


QCMetricCallable = Callable[[pd.DataFrame, QCExecutionContext], QCMetricResult]


class QCMetricRegistry:
    """Registry of named QC metric callables."""

    def __init__(self) -> None:
        self._entries: MutableMapping[str, QCMetricCallable] = {}
        self._lock = RLock()

    def register(self, name: str, func: QCMetricCallable, *, override: bool = False) -> None:
        """Register a new QC metric callable."""

        if not name:
            msg = "metric name must be a non-empty string"
            raise ValueError(msg)
        with self._lock:
            if not override and name in self._entries:
                msg = f"metric {name!r} is already registered"
                raise ValueError(msg)
            self._entries[name] = func

    def unregister(self, name: str) -> None:
        """Remove a previously registered QC metric."""

        with self._lock:
            self._entries.pop(name, None)

    def get(self, name: str) -> QCMetricCallable:
        """Return a registered QC metric callable."""

        with self._lock:
            try:
                return self._entries[name]
            except KeyError as exc:  # pragma: no cover - defensive branch
                msg = f"QC metric {name!r} is not registered"
                raise KeyError(msg) from exc

    def list_metrics(self) -> tuple[str, ...]:
        """Return registered metric names in deterministic order."""

        with self._lock:
            return tuple(sorted(self._entries))


QC_METRIC_REGISTRY = QCMetricRegistry()


def register_qc_metric(name: str, func: QCMetricCallable, *, override: bool = False) -> None:
    """Convenience wrapper around :class:`QCMetricRegistry.register`."""

    QC_METRIC_REGISTRY.register(name, func, override=override)


class QCMetricsExecutor:
    """Execute QC plans and aggregate their results."""

    def __init__(self, registry: QCMetricRegistry | None = None) -> None:
        self._registry = registry or QC_METRIC_REGISTRY

    def execute(
        self,
        df: pd.DataFrame,
        *,
        plan: QCPlan | None = None,
        business_key_fields: Sequence[str] | None = None,
        extra_metrics: Sequence[QCMetricCallable] | None = None,
    ) -> QCMetricsBundle:
        """Execute ``plan`` against ``df`` and return aggregated metrics."""

        effective_plan = plan or QC_PLAN_DEFAULT
        key_fields = tuple(business_key_fields or ())

        from bioetl.core.io.units import QCUnits as _QCUnits

        duplicates = (
            compute_duplicate_stats(df, business_key_fields=key_fields or None)
            if effective_plan.duplicates
            else None
        )
        missingness = compute_missingness(df) if effective_plan.missingness else None
        units_distribution = (
            _QCUnits.for_units(df) if effective_plan.units_distribution else None
        )
        relation_distribution = (
            _QCUnits.for_relation(df) if effective_plan.relation_distribution else None
        )
        correlation = compute_correlation_matrix(df) if effective_plan.correlation else None
        outliers = (
            detect_iqr_outliers(
                df,
                multiplier=effective_plan.outlier_iqr_multiplier,
                min_count=effective_plan.outlier_min_count,
            )
            if effective_plan.outliers
            else ()
        )

        base_bundle = QCMetricsBundle(
            duplicates=duplicates,
            missingness=missingness,
            units_distribution=units_distribution,
            relation_distribution=relation_distribution,
            correlation=correlation,
            outliers=outliers,
            custom={},
            business_key_fields=key_fields,
        )

        if not effective_plan.custom_metrics and not extra_metrics:
            return base_bundle

        context = QCExecutionContext(
            plan=effective_plan,
            business_key_fields=key_fields,
            bundle=base_bundle,
        )
        custom_results: dict[str, QCMetricResult] = {}
        for name in effective_plan.custom_metrics:
            metric = self._registry.get(name)
            result = metric(df, context)
            custom_results[result.name] = result

        if extra_metrics:
            for metric in extra_metrics:
                result = metric(df, context)
                custom_results[result.name] = result

        return QCMetricsBundle(
            duplicates=base_bundle.duplicates,
            missingness=base_bundle.missingness,
            units_distribution=base_bundle.units_distribution,
            relation_distribution=base_bundle.relation_distribution,
            correlation=base_bundle.correlation,
            outliers=base_bundle.outliers,
            custom=custom_results,
            business_key_fields=base_bundle.business_key_fields,
        )


__all__ = [
    "QCMetricCallable",
    "QCMetricRegistry",
    "QCMetricResult",
    "QCExecutionContext",
    "QCPlan",
    "QC_PLAN_DEFAULT",
    "QCMetricsBundle",
    "QCMetricsExecutor",
    "QC_METRIC_REGISTRY",
    "register_qc_metric",
]

