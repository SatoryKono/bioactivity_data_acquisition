"""Quality control utilities: metric computation and threshold evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Iterable, Mapping

import pandas as pd

from bioetl.config.models import QCConfig, QCThresholdConfig


class SeverityLevel(IntEnum):
    """Ordered severity levels used by QC policies."""

    INFO = 0
    WARNING = 1
    ERROR = 2

    @classmethod
    def from_str(cls, value: str) -> "SeverityLevel":
        """Create severity level from string representation."""
        normalized = value.lower()
        try:
            return cls[normalized.upper()]
        except KeyError as exc:
            raise ValueError(f"Unsupported severity level: {value}") from exc

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.name.lower()


@dataclass(frozen=True)
class QCThresholdViolation:
    """Represents a metric that violated configured thresholds."""

    metric: str
    value: float
    threshold: QCThresholdConfig
    condition: str
    severity: SeverityLevel

    @property
    def message(self) -> str:
        """Human readable message for logging and errors."""
        return (
            f"{self.metric}={self.value:.4f} violates {self.condition} "
            f"(severity={self.threshold.severity})"
        )


def compute_document_qc_metrics(df: pd.DataFrame) -> dict[str, float]:
    """Compute coverage and conflict metrics for the document pipeline."""

    total = len(df)

    def ratio(mask: pd.Series) -> float:
        if total == 0:
            return 0.0
        normalized_mask = mask.fillna(False)
        return float(normalized_mask.sum()) / float(total)

    metrics: dict[str, float] = {}

    metrics["doi_coverage"] = ratio(df["doi_clean"].notna()) if "doi_clean" in df else 0.0
    metrics["pmid_coverage"] = ratio(df["pubmed_id"].notna()) if "pubmed_id" in df else 0.0
    metrics["title_coverage"] = ratio(df["title"].notna()) if "title" in df else 0.0
    metrics["journal_coverage"] = ratio(df["journal"].notna()) if "journal" in df else 0.0
    metrics["authors_coverage"] = ratio(df["authors"].notna()) if "authors" in df else 0.0

    if "conflict_doi" in df:
        metrics["conflicts_doi"] = ratio(df["conflict_doi"] == True)  # noqa: E712
    else:
        metrics["conflicts_doi"] = 0.0

    if "conflict_pmid" in df:
        metrics["conflicts_pmid"] = ratio(df["conflict_pmid"] == True)  # noqa: E712
    else:
        metrics["conflicts_pmid"] = 0.0

    if "qc_flag_title_fallback_used" in df:
        metrics["title_fallback_rate"] = ratio(df["qc_flag_title_fallback_used"] == 1)
    else:
        metrics["title_fallback_rate"] = 0.0

    return metrics


def evaluate_thresholds(
    metrics: Mapping[str, float], qc_config: QCConfig
) -> list[QCThresholdViolation]:
    """Evaluate QC thresholds and return violations."""

    violations: list[QCThresholdViolation] = []

    for metric, threshold in qc_config.thresholds.items():
        if metric not in metrics:
            continue

        value = metrics[metric]
        severity = SeverityLevel.from_str(threshold.severity)

        if threshold.min is not None and value < threshold.min:
            violations.append(
                QCThresholdViolation(
                    metric=metric,
                    value=value,
                    threshold=threshold,
                    condition=f"min={threshold.min}",
                    severity=severity,
                )
            )

        if threshold.max is not None and value > threshold.max:
            violations.append(
                QCThresholdViolation(
                    metric=metric,
                    value=value,
                    threshold=threshold,
                    condition=f"max={threshold.max}",
                    severity=severity,
                )
            )

    return violations


def should_fail(
    violations: Iterable[QCThresholdViolation],
    severity_threshold: SeverityLevel,
) -> bool:
    """Determine if any violation requires aborting the pipeline."""

    return any(violation.severity >= severity_threshold for violation in violations)


def build_qc_report(
    metrics: Mapping[str, float],
    thresholds: Mapping[str, QCThresholdConfig],
    violations: Iterable[QCThresholdViolation],
) -> pd.DataFrame:
    """Build a QC summary DataFrame suitable for export."""

    violation_map = {violation.metric: violation for violation in violations}
    rows: list[dict[str, object]] = []

    for metric, value in metrics.items():
        threshold = thresholds.get(metric)
        violation = violation_map.get(metric)
        rows.append(
            {
                "metric": metric,
                "value": value,
                "min_threshold": getattr(threshold, "min", None),
                "max_threshold": getattr(threshold, "max", None),
                "severity": getattr(threshold, "severity", None),
                "status": "violation" if violation else "ok",
                "message": violation.message if violation else None,
            }
        )

    return pd.DataFrame(rows)


__all__ = [
    "SeverityLevel",
    "QCThresholdViolation",
    "compute_document_qc_metrics",
    "evaluate_thresholds",
    "should_fail",
    "build_qc_report",
]
