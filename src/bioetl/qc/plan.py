from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator


class MetricSpec(BaseModel):
    """Declarative description of a QC metric."""

    name: str
    type: str
    params: dict[str, Any] = Field(default_factory=dict)
    group_by: list[str] | None = None
    executor: str | None = None


class QCPlan(BaseModel):
    """Configuration-driven QC plan loaded from pipeline settings."""

    metrics: list[MetricSpec] = Field(default_factory=list)
    thresholds: dict[str, float] = Field(default_factory=dict)
    report_templates: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True
    fail_on_threshold_violation: bool = False
    dry_run: bool = False

    @model_validator(mode="after")
    def _ensure_unique_metrics(self) -> "QCPlan":
        names = [metric.name for metric in self.metrics]
        if len(names) != len(set(names)):
            duplicates = {name for name in names if names.count(name) > 1}
            raise ValueError(f"duplicate metric definitions found: {sorted(duplicates)}")
        return self

    @classmethod
    def with_default_metrics(cls) -> "QCPlan":
        """Provide a minimal default QC plan used by the CLI."""

        defaults = [
            MetricSpec(name="row_count", type="row_count"),
            MetricSpec(name="null_percentage", type="null_percentage"),
            MetricSpec(name="unique_key_count", type="unique_count", params={"column": "id"}),
        ]
        return cls(metrics=defaults)


__all__ = ["MetricSpec", "QCPlan"]
