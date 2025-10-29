"""Shared QC metric representations and registry helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable


@dataclass(slots=True)
class QcMetric:
    """Concrete QC metric with normalised metadata."""

    name: str
    value: Any
    passed: bool = True
    severity: str = "info"
    threshold: float | None = None
    threshold_min: float | None = None
    threshold_max: float | None = None
    count: int | None = None
    details: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    fail_severity: str | None = None

    def apply_policy(self, policy: "QcMetricPolicy") -> None:
        """Merge registry policy into the metric in-place."""

        if policy.severity:
            self.severity = str(policy.severity)

        if policy.fail_severity:
            self.fail_severity = str(policy.fail_severity)

        if policy.min_value is not None and self.threshold_min is None:
            self.threshold_min = float(policy.min_value)
        if policy.max_value is not None and self.threshold_max is None:
            self.threshold_max = float(policy.max_value)
        if policy.threshold is not None and self.threshold is None:
            self.threshold = float(policy.threshold)

        if self.threshold is None and self.threshold_max is not None:
            self.threshold = self.threshold_max

        if policy.metadata:
            self.metadata.update(policy.metadata)

        # Re-evaluate pass/fail state if we have numeric bounds.
        numeric_value: float | None = None
        if isinstance(self.value, (int, float)):
            numeric_value = float(self.value)
        elif isinstance(self.value, str):
            try:
                numeric_value = float(self.value)
            except ValueError:
                numeric_value = None

        if numeric_value is not None:
            if self.threshold_min is not None and numeric_value < float(self.threshold_min):
                self.passed = False
            if self.threshold_max is not None and numeric_value > float(self.threshold_max):
                self.passed = False
            if self.threshold is not None and self.threshold_max is None:
                if numeric_value > float(self.threshold):
                    self.passed = False

    def to_summary(self) -> dict[str, Any]:
        """Return a serialisable representation used in QC outputs."""

        summary: dict[str, Any] = {
            "value": self.value,
            "passed": self.passed,
            "severity": self.severity,
        }
        if self.count is not None:
            summary["count"] = self.count
        if self.threshold is not None:
            summary["threshold"] = self.threshold
        if self.threshold_min is not None:
            summary["threshold_min"] = self.threshold_min
        if self.threshold_max is not None:
            summary["threshold_max"] = self.threshold_max
        if self.details is not None:
            summary["details"] = self.details
        if self.metadata:
            summary["metadata"] = self.metadata
        if self.fail_severity is not None:
            summary["fail_severity"] = self.fail_severity
        return summary

    def to_issue_payload(self) -> dict[str, Any]:
        """Generate payload suitable for :meth:`PipelineBase.record_validation_issue`."""

        payload = {
            "metric": self.name,
            "issue_type": "qc_metric",
            "severity": self.severity,
            "value": self.value,
            "passed": self.passed,
        }
        if self.threshold is not None:
            payload["threshold"] = self.threshold
        if self.threshold_min is not None:
            payload["threshold_min"] = self.threshold_min
        if self.threshold_max is not None:
            payload["threshold_max"] = self.threshold_max
        if self.count is not None:
            payload["count"] = self.count
        if self.details is not None:
            payload["details"] = self.details
        if self.fail_severity is not None:
            payload["fail_severity"] = self.fail_severity
        if self.metadata:
            payload["metadata"] = self.metadata
        return payload


@dataclass(slots=True)
class QcMetricPolicy:
    """Declarative policy derived from configuration for a QC metric."""

    min_value: float | None = None
    max_value: float | None = None
    threshold: float | None = None
    severity: str | None = None
    fail_severity: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class QcMetricsRegistry:
    """Registry resolving QC metric policies and storing metric state."""

    def __init__(self, thresholds: dict[str, Any] | None = None):
        self._thresholds = thresholds or {}
        self._metrics: dict[str, QcMetric] = {}
        self._policies: dict[str, QcMetricPolicy] = {}

    def clear(self) -> None:
        """Reset all registered metrics."""

        self._metrics.clear()
        self._policies.clear()

    # A resolver ensures we can patch in global severity map during evaluation later.
    def register(self, metric: QcMetric) -> QcMetric:
        """Register a QC metric and apply configuration policy."""

        policy = self._resolve_policy(metric.name)
        metric.apply_policy(policy)
        self._metrics[metric.name] = metric
        self._policies[metric.name] = policy
        return metric

    def iter_metrics(self) -> Iterable[QcMetric]:
        """Iterate through registered metrics in insertion order."""

        return self._metrics.values()

    def as_dict(self) -> dict[str, dict[str, Any]]:
        """Return serialisable mapping of metrics for QC outputs."""

        return {name: metric.to_summary() for name, metric in self._metrics.items()}

    def get(self, name: str) -> QcMetric | None:
        """Return metric by name if present."""

        return self._metrics.get(name)

    def failing_metrics(
        self,
        *,
        severity_threshold: str,
        severity_resolver: Callable[[str], int],
    ) -> dict[str, QcMetric]:
        """Return metrics that failed and breach the provided severity threshold."""

        failing: dict[str, QcMetric] = {}
        threshold_value = severity_resolver(severity_threshold)
        for metric in self._metrics.values():
            if metric.passed:
                continue
            local_threshold = metric.fail_severity or severity_threshold
            if severity_resolver(metric.severity) >= severity_resolver(local_threshold):
                failing[metric.name] = metric
            elif severity_resolver(metric.severity) >= threshold_value:
                failing[metric.name] = metric
        return failing

    def _resolve_policy(self, name: str) -> QcMetricPolicy:
        """Build a policy from the configured threshold structure."""

        raw_policy = self._thresholds.get(name)
        if raw_policy is None:
            return QcMetricPolicy()

        if isinstance(raw_policy, QcMetricPolicy):
            return raw_policy

        if isinstance(raw_policy, (int, float)):
            return QcMetricPolicy(max_value=float(raw_policy))

        if isinstance(raw_policy, str):
            try:
                numeric_value = float(raw_policy)
            except ValueError:
                return QcMetricPolicy(severity=raw_policy)
            return QcMetricPolicy(max_value=numeric_value)

        if isinstance(raw_policy, dict):
            policy = QcMetricPolicy(
                min_value=self._coerce_optional_float(raw_policy.get("min")),
                max_value=self._coerce_optional_float(raw_policy.get("max")),
                threshold=self._coerce_optional_float(raw_policy.get("threshold")),
                severity=str(raw_policy.get("severity")) if raw_policy.get("severity") else None,
                fail_severity=str(raw_policy.get("fail_severity"))
                if raw_policy.get("fail_severity")
                else None,
            )
            metadata = {
                key: value
                for key, value in raw_policy.items()
                if key
                not in {
                    "min",
                    "max",
                    "threshold",
                    "severity",
                    "fail_severity",
                }
            }
            if metadata:
                policy.metadata = metadata
            return policy

        return QcMetricPolicy()

    @staticmethod
    def _coerce_optional_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
