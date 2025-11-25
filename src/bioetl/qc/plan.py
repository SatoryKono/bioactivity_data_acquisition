from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd

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


class QCMetricsExecutor:
    """Minimal executor placeholder used by pipelines."""

    def execute(
        self,
        df,
        *,
        plan: QCPlan | None = None,
        business_key_fields=None,
        dataset_name: str = "dataset",
        output_dir=None,
    ):
        base_dir = Path(output_dir) if output_dir else Path(".")
        base_dir.mkdir(parents=True, exist_ok=True)
        quality_report = base_dir / f"{dataset_name}_quality_report.csv"
        pd.DataFrame(
            [
                {
                    "dataset": dataset_name,
                    "metric": "row_count",
                    "metric_type": "custom",
                    "value": len(df),
                    "threshold": None,
                    "status": "PASS",
                    "message": None,
                }
            ]
        ).to_csv(quality_report, index=False)
        return SimpleNamespace(report_paths={"quality_report": quality_report})


__all__ = ["MetricSpec", "QCPlan", "QCMetricsExecutor"]
