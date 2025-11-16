"""Runtime-level configuration helpers used by the CLI layer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

DEFAULT_CONFIG_PATH = Path("configs/default.yml")


@dataclass(frozen=True)
class QCReportRuntimeOptions:
    """Resolved runtime options describing QC report layout for a pipeline."""

    pipeline_code: str
    directory: Path
    quality_template: str
    correlation_template: str
    metrics_template: str

    def _format(self, template: str, stem: str) -> Path:
        filename = template.format(stem=stem, pipeline=self.pipeline_code)
        return (self.directory / filename).expanduser()

    def quality_path(self, *, stem: str) -> Path:
        """Return the target path for the quality report."""

        return self._format(self.quality_template, stem)

    def correlation_path(self, *, stem: str) -> Path:
        """Return the target path for the correlation report."""

        return self._format(self.correlation_template, stem)

    def metrics_path(self, *, stem: str) -> Path:
        """Return the target path for aggregated QC metrics."""

        return self._format(self.metrics_template, stem)


class QCPipelineReportOverride(BaseModel):
    """Optional per-pipeline overrides for QC report layout."""

    model_config = ConfigDict(extra="forbid")

    directory: str | None = Field(default=None)
    quality_template: str | None = Field(default=None)
    correlation_template: str | None = Field(default=None)
    metrics_template: str | None = Field(default=None)


class QCReportsConfig(BaseModel):
    """Configuration describing QC report directories and file templates."""

    model_config = ConfigDict(extra="forbid")

    directory: str = Field(default="{materialization_root}/qc")
    quality_template: str = Field(default="{stem}_quality_report.csv")
    correlation_template: str = Field(default="{stem}_correlation_report.csv")
    metrics_template: str = Field(default="{stem}_qc.csv")
    pipelines: dict[str, QCPipelineReportOverride] = Field(default_factory=dict)

    def build(self, *, pipeline: str, materialization_root: Path) -> QCReportRuntimeOptions:
        """Return resolved report options for ``pipeline``."""

        override = self.pipelines.get(pipeline)
        directory_template = override.directory if override and override.directory else self.directory
        quality_template = (
            override.quality_template if override and override.quality_template else self.quality_template
        )
        correlation_template = (
            override.correlation_template
            if override and override.correlation_template
            else self.correlation_template
        )
        metrics_template = (
            override.metrics_template if override and override.metrics_template else self.metrics_template
        )

        mapping = {
            "pipeline": pipeline,
            "materialization_root": str(materialization_root),
            "output_root": str(materialization_root),
        }
        directory = Path(directory_template.format(**mapping)).expanduser().resolve()

        return QCReportRuntimeOptions(
            pipeline_code=pipeline,
            directory=directory,
            quality_template=quality_template,
            correlation_template=correlation_template,
            metrics_template=metrics_template,
        )


class QCThresholdValues(BaseModel):
    """Typed thresholds shared across pipelines with optional overrides."""

    model_config = ConfigDict(extra="forbid")

    duplicate_ratio: float = Field(default=0.0, ge=0.0, le=1.0)
    missing_value_ratio: float = Field(default=0.05, ge=0.0, le=1.0)
    fallback_usage_rate: float = Field(default=0.1, ge=0.0, le=1.0)
    identifier_coverage: float = Field(default=0.95, ge=0.0, le=1.0)
    min_doi_coverage: float = Field(default=0.3, ge=0.0, le=1.0)
    max_year_out_of_range: float = Field(default=0.01, ge=0.0, le=1.0)
    max_s2_access_denied: float = Field(default=0.05, ge=0.0, le=1.0)
    max_title_fallback: float = Field(default=0.1, ge=0.0, le=1.0)
    pipelines: dict[str, dict[str, float]] = Field(default_factory=dict)

    @field_validator("pipelines", mode="before")
    @classmethod
    def _coerce_pipeline_thresholds(cls, value: Any) -> dict[str, dict[str, float]]:
        if value is None:
            return {}
        if not isinstance(value, Mapping):
            msg = "pipelines thresholds must be a mapping"
            raise TypeError(msg)
        normalized: dict[str, dict[str, float]] = {}
        for pipeline, payload in value.items():
            if not isinstance(payload, Mapping):
                msg = f"threshold override for pipeline {pipeline!r} must be a mapping"
                raise TypeError(msg)
            normalized[pipeline] = {str(key): float(val) for key, val in payload.items()}
        return normalized

    def base_thresholds(self) -> dict[str, float]:
        """Return the global thresholds without pipeline overrides."""

        return {
            "duplicate_ratio": float(self.duplicate_ratio),
            "missing_value_ratio": float(self.missing_value_ratio),
            "fallback_usage_rate": float(self.fallback_usage_rate),
            "identifier_coverage": float(self.identifier_coverage),
            "min_doi_coverage": float(self.min_doi_coverage),
            "max_year_out_of_range": float(self.max_year_out_of_range),
            "max_s2_access_denied": float(self.max_s2_access_denied),
            "max_title_fallback": float(self.max_title_fallback),
        }

    def for_pipeline(self, pipeline: str | None) -> dict[str, float]:
        """Return thresholds for ``pipeline`` merged with global defaults."""

        merged = self.base_thresholds()
        if pipeline is None:
            return merged
        overrides = self.pipelines.get(pipeline)
        if overrides:
            merged.update({key: float(value) for key, value in overrides.items()})
        return merged


class QCConfig(BaseModel):
    """Grouping of QC feature toggles, thresholds, and report settings."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=True)
    fail_on_threshold_violation: bool = Field(default=False)
    thresholds: QCThresholdValues = Field(default_factory=QCThresholdValues)
    reports: QCReportsConfig = Field(default_factory=QCReportsConfig)

    def thresholds_for(self, pipeline: str | None = None) -> dict[str, float]:
        return self.thresholds.for_pipeline(pipeline)

    def reports_for(self, pipeline: str, *, materialization_root: Path) -> QCReportRuntimeOptions:
        return self.reports.build(pipeline=pipeline, materialization_root=materialization_root)

    @property
    def duplicate_ratio(self) -> float:
        return self.thresholds.duplicate_ratio

    @property
    def missing_value_ratio(self) -> float:
        return self.thresholds.missing_value_ratio

    @property
    def fallback_usage_rate(self) -> float:
        return self.thresholds.fallback_usage_rate

    @property
    def identifier_coverage(self) -> float:
        return self.thresholds.identifier_coverage

    @property
    def min_doi_coverage(self) -> float:
        return self.thresholds.min_doi_coverage

    @property
    def max_year_out_of_range(self) -> float:
        return self.thresholds.max_year_out_of_range

    @property
    def max_s2_access_denied(self) -> float:
        return self.thresholds.max_s2_access_denied

    @property
    def max_title_fallback(self) -> float:
        return self.thresholds.max_title_fallback


class Config(BaseModel):
    """Runtime configuration root model."""

    model_config = ConfigDict(extra="forbid")

    version: int = Field(default=1)
    qc: QCConfig = Field(default_factory=QCConfig)

    @classmethod
    def load(cls, path: str | Path | None = None) -> "Config":
        """Load configuration from ``path`` (defaults to ``configs/default.yml``)."""

        config_path = Path(path or DEFAULT_CONFIG_PATH)
        if not config_path.exists():
            msg = f"Runtime configuration file not found: {config_path}"
            raise FileNotFoundError(msg)
        with config_path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
        return cls.model_validate(payload)

    def thresholds_for(self, pipeline: str | None = None) -> dict[str, float]:
        """Expose QC thresholds merged with optional pipeline overrides."""

        return self.qc.thresholds_for(pipeline)

    def reports_for(self, pipeline: str, *, materialization_root: Path) -> QCReportRuntimeOptions:
        """Return QC report layout for ``pipeline``."""

        return self.qc.reports_for(pipeline, materialization_root=materialization_root)


__all__ = [
    "Config",
    "DEFAULT_CONFIG_PATH",
    "QCConfig",
    "QCReportRuntimeOptions",
    "QCReportsConfig",
    "QCThresholdValues",
]

