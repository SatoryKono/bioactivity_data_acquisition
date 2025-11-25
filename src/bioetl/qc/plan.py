from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from typing import Any, Mapping
from collections.abc import Callable, MutableMapping, Sequence

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class QCPlan:
    """Declarative QC plan controlling which metrics to compute."""

    duplicates: bool = True
    missingness: bool = True
    correlation: bool = True
    outliers: bool = True
    custom_metrics: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        duplicates = set()
        for name in self.custom_metrics:
            if name in duplicates:
                raise ValueError(f"duplicate custom metric name: {name}")
            duplicates.add(name)


@dataclass(frozen=True)
class QCMetricResult:
    name: str
    payload: Any
    section: str = "custom"
    rows: tuple[dict[str, Any], ...] = ()
    metadata: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class QCMetricsBundle:
    duplicates: Mapping[str, Any] | None = None
    missingness: pd.DataFrame | None = None
    correlation: pd.DataFrame | None = None
    outliers: pd.DataFrame | None = None
    custom: Mapping[str, QCMetricResult] = field(default_factory=dict)
    report_paths: Mapping[str, Path] | None = None


@dataclass(frozen=True)
class QCExecutionContext:
    plan: QCPlan
    bundle: QCMetricsBundle


QCMetricCallable = Callable[[pd.DataFrame, QCExecutionContext], QCMetricResult]


class QCMetricRegistry:
    """Registry for custom QC metric callables."""

    def __init__(self) -> None:
        self._entries: MutableMapping[str, QCMetricCallable] = {}
        self._lock = RLock()

    def register(self, name: str, func: QCMetricCallable, *, override: bool = False) -> None:
        if not name:
            raise ValueError("metric name must be provided")
        with self._lock:
            if name in self._entries and not override:
                raise ValueError(f"metric {name!r} already registered")
            self._entries[name] = func

    def unregister(self, name: str) -> None:
        with self._lock:
            self._entries.pop(name, None)

    def get(self, name: str) -> QCMetricCallable:
        with self._lock:
            try:
                return self._entries[name]
            except KeyError as exc:
                raise KeyError(f"metric {name!r} is not registered") from exc

    def list_metrics(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._entries))


QC_METRIC_REGISTRY = QCMetricRegistry()


def register_qc_metric(name: str, func: QCMetricCallable, *, override: bool = False) -> None:
    QC_METRIC_REGISTRY.register(name, func, override=override)


class QCMetricsExecutor:
    """Compute QC metrics and optionally persist human-readable reports."""

    def __init__(self, registry: QCMetricRegistry | None = None) -> None:
        self._registry = registry or QC_METRIC_REGISTRY

    def execute(
        self,
        df: pd.DataFrame,
        *,
        plan: QCPlan | None = None,
        business_key_fields: Sequence[str] | None = None,
        extra_metrics: Sequence[QCMetricCallable] | None = None,
        dataset_name: str = "dataset",
        output_dir: Path | None = None,
    ) -> QCMetricsBundle:
        effective_plan = plan or QCPlan()

        duplicates = (
            _compute_duplicates(df, business_key_fields=business_key_fields)
            if effective_plan.duplicates
            else None
        )
        missingness = _compute_missingness(df) if effective_plan.missingness else None
        correlation = _compute_correlation(df) if effective_plan.correlation else None
        outliers = _compute_outliers(df) if effective_plan.outliers else None

        base_bundle = QCMetricsBundle(
            duplicates=duplicates,
            missingness=missingness,
            correlation=correlation,
            outliers=outliers,
        )

        context = QCExecutionContext(plan=effective_plan, bundle=base_bundle)
        custom_results: dict[str, QCMetricResult] = {}

        for name in effective_plan.custom_metrics:
            metric = self._registry.get(name)
            result = metric(df, context)
            custom_results[result.name] = result

        for metric in extra_metrics or ():
            result = metric(df, context)
            custom_results[result.name] = result

        report_paths: dict[str, Path] | None = None
        if output_dir is not None:
            report_paths = self._write_reports(
                df,
                bundle=base_bundle,
                custom=custom_results,
                dataset_name=dataset_name,
                output_dir=output_dir,
            )

        return QCMetricsBundle(
            duplicates=base_bundle.duplicates,
            missingness=base_bundle.missingness,
            correlation=base_bundle.correlation,
            outliers=base_bundle.outliers,
            custom=custom_results,
            report_paths=report_paths,
        )

    def _write_reports(
        self,
        df: pd.DataFrame,
        *,
        bundle: QCMetricsBundle,
        custom: Mapping[str, QCMetricResult],
        dataset_name: str,
        output_dir: Path,
    ) -> dict[str, Path]:
        output_dir.mkdir(parents=True, exist_ok=True)
        quality_report_path = output_dir / f"{dataset_name}_quality_report.csv"
        qc_json_path = output_dir / f"{dataset_name}_qc.json"
        correlation_path = output_dir / f"{dataset_name}_correlation_report.csv"

        quality_sections: list[pd.DataFrame] = []
        if bundle.missingness is not None:
            missing_copy = bundle.missingness.copy()
            missing_copy.insert(0, "metric", "missingness")
            quality_sections.append(missing_copy)
        if bundle.outliers is not None:
            outlier_copy = bundle.outliers.copy()
            outlier_copy.insert(0, "metric", "outliers")
            quality_sections.append(outlier_copy)

        if quality_sections:
            pd.concat(quality_sections, ignore_index=True).to_csv(
                quality_report_path, index=False
            )

        summary_payload: dict[str, Any] = {
            "rows": int(len(df)),
            "columns": list(df.columns),
            "custom_metrics": list(custom),
        }
        if bundle.duplicates is not None:
            summary_payload["duplicates"] = dict(bundle.duplicates)
        if bundle.missingness is not None:
            summary_payload["missingness"] = bundle.missingness.to_dict(orient="records")
        if bundle.outliers is not None:
            summary_payload["outliers"] = bundle.outliers.to_dict(orient="records")
        if custom:
            summary_payload["custom"] = {
                name: result.payload for name, result in custom.items()
            }

        qc_json_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

        if bundle.correlation is not None and not bundle.correlation.empty:
            bundle.correlation.to_csv(correlation_path, index=True)

        return {
            "quality_report": quality_report_path,
            "qc_json": qc_json_path,
            "correlation_report": correlation_path,
        }


def _compute_duplicates(df: pd.DataFrame, *, business_key_fields: Sequence[str] | None) -> dict[str, Any]:
    total_rows = int(len(df))
    subset = [col for col in (business_key_fields or []) if col in df.columns]
    deduped = df.drop_duplicates(subset=subset or None, keep="first")
    duplicate_count = total_rows - int(len(deduped))
    return {
        "rows": total_rows,
        "duplicate_count": duplicate_count,
        "duplicate_ratio": float(duplicate_count / total_rows) if total_rows else 0.0,
    }


def _compute_missingness(df: pd.DataFrame) -> pd.DataFrame:
    missing_counts = df.isna().sum()
    ratios = missing_counts / len(df) if len(df) else 0
    payload = pd.DataFrame(
        {
            "column": missing_counts.index,
            "missing_count": missing_counts.values,
            "missing_ratio": ratios.values,
        }
    )
    return payload.sort_values("column").reset_index(drop=True)


def _compute_correlation(df: pd.DataFrame) -> pd.DataFrame:
    numeric_df = df.select_dtypes(include=[np.number])
    if numeric_df.empty:
        return pd.DataFrame()
    return numeric_df.corr(numeric_only=True)


def _compute_outliers(df: pd.DataFrame) -> pd.DataFrame:
    numeric_df = df.select_dtypes(include=[np.number])
    rows: list[dict[str, Any]] = []
    for column in numeric_df.columns:
        series = numeric_df[column].dropna()
        if series.empty:
            continue
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            continue
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        mask = (series < lower) | (series > upper)
        rows.append(
            {
                "column": column,
                "outlier_count": int(mask.sum()),
                "ratio": float(mask.mean()) if len(series) else 0.0,
                "lower_bound": float(lower),
                "upper_bound": float(upper),
            }
        )
    return pd.DataFrame(rows)


__all__ = [
    "QCPlan",
    "QCMetricsBundle",
    "QCMetricsExecutor",
    "QCMetricCallable",
    "QCMetricRegistry",
    "QCMetricResult",
    "QCExecutionContext",
    "QC_METRIC_REGISTRY",
    "register_qc_metric",
]
