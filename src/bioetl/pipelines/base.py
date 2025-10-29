"""Base pipeline class."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable

import pandas as pd
from pandera.errors import SchemaErrors

from bioetl.config import PipelineConfig
from bioetl.core.logger import UnifiedLogger
from bioetl.core.output_writer import (
    AdditionalTableSpec,
    OutputArtifacts,
    OutputMetadata,
    UnifiedOutputWriter,
)
from bioetl.qc import QcMetric, QcMetricsRegistry

logger = UnifiedLogger.get(__name__)


class PipelineBase(ABC):
    """Базовый класс для всех пайплайнов."""

    def __init__(self, config: PipelineConfig, run_id: str):
        self.config = config
        self.run_id = run_id
        self.output_writer = UnifiedOutputWriter(run_id)
        self.validation_issues: list[dict[str, Any]] = []
        self.qc_metrics: dict[str, Any] = {}
        self.qc_summary_data: dict[str, Any] = {}
        self.qc_metric_registry = QcMetricsRegistry(config.qc.thresholds)
        self.qc_missing_mappings = pd.DataFrame()
        self.qc_enrichment_metrics = pd.DataFrame()
        self.runtime_options: dict[str, Any] = {}
        self.additional_tables: dict[str, AdditionalTableSpec] = {}
        self.export_metadata: OutputMetadata | None = None
        self.debug_dataset_path: Path | None = None
        logger.info("pipeline_initialized", pipeline=config.pipeline.name, run_id=run_id)

    _SEVERITY_LEVELS: dict[str, int] = {"info": 0, "warning": 1, "error": 2, "critical": 3}

    def record_validation_issue(self, issue: dict[str, Any]) -> None:
        """Store a validation issue for later QC reporting."""

        issue = issue.copy()
        issue.setdefault("metric", "validation_issue")
        issue.setdefault("severity", "info")
        self.validation_issues.append(issue)

    def get_runtime_limit(self) -> int | None:
        """Return a positive runtime limit if configured, normalising the value."""

        raw_limit = self.runtime_options.get("limit")
        if raw_limit is None:
            raw_limit = self.runtime_options.get("sample")
        if raw_limit is None:
            return None

        try:
            limit_value = int(raw_limit)
        except (TypeError, ValueError):
            logger.warning(
                "invalid_runtime_limit",
                pipeline=self.config.pipeline.name,
                limit=raw_limit,
            )
            return None

        if limit_value < 1:
            logger.warning(
                "non_positive_runtime_limit",
                pipeline=self.config.pipeline.name,
                limit=limit_value,
            )
            return None

        self.runtime_options["limit"] = limit_value
        return limit_value

    def _severity_value(self, severity: str) -> int:
        """Convert severity label to comparable integer."""

        return self._SEVERITY_LEVELS.get(severity.lower(), 0)

    def _should_fail(self, severity: str, *, threshold: str | None = None) -> bool:
        """Determine if the given severity breaches the configured threshold."""

        threshold_label = threshold or self.config.qc.severity_threshold
        return self._severity_value(severity) >= self._severity_value(threshold_label)

    def clear_qc_metrics(self) -> None:
        """Reset QC metrics prior to a fresh validation pass."""

        self.qc_metrics.clear()
        self.qc_metric_registry.clear()
        self.qc_summary_data.setdefault("metrics", {})
        self.qc_summary_data["metrics"] = {}

    def register_qc_metric(
        self,
        name: str,
        value: Any,
        *,
        passed: bool | None = None,
        severity: str = "info",
        threshold: float | None = None,
        threshold_min: float | None = None,
        threshold_max: float | None = None,
        count: int | None = None,
        details: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        fail_severity: str | None = None,
    ) -> QcMetric:
        """Normalise and persist QC metric state for downstream reporting."""

        metric = QcMetric(
            name=name,
            value=value,
            passed=True if passed is None else bool(passed),
            severity=severity,
            threshold=threshold,
            threshold_min=threshold_min,
            threshold_max=threshold_max,
            count=count,
            details=details,
            metadata=metadata or {},
            fail_severity=fail_severity,
        )
        metric = self.qc_metric_registry.register(metric)
        summary = metric.to_summary()
        self.qc_metrics[name] = summary
        self.qc_summary_data.setdefault("metrics", {})[name] = summary
        self.record_validation_issue(metric.to_issue_payload())
        logger.debug(
            "qc_metric_registered",
            metric=name,
            value=value,
            passed=metric.passed,
            severity=metric.severity,
        )
        return metric

    def enforce_qc_metrics(
        self,
        *,
        severity_threshold: str | None = None,
        raise_exception: bool = True,
    ) -> dict[str, QcMetric]:
        """Return failing metrics and optionally raise when the severity threshold is hit."""

        effective_threshold = severity_threshold or self.config.qc.severity_threshold
        failing = self.qc_metric_registry.failing_metrics(
            severity_threshold=effective_threshold,
            severity_resolver=self._severity_value,
        )
        if failing and raise_exception:
            logger.error(
                "qc_threshold_exceeded",
                failing={name: metric.to_summary() for name, metric in failing.items()},
                threshold=effective_threshold,
            )
            metric_list = ", ".join(sorted(failing))
            raise ValueError(f"QC thresholds exceeded for metrics: {metric_list}")
        return failing

    @abstractmethod
    def extract(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """Извлекает данные из источника."""
        pass

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Трансформирует данные."""
        pass

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Валидирует данные через Pandera."""
        # Placeholder for now
        return df

    def _resolve_schema_policy(
        self, dataset_name: str, *, severity: str
    ) -> tuple[str, str | None]:
        """Return severity override and fail threshold for schema validation."""

        policy = self.config.qc.schema_thresholds.get(dataset_name, {})
        severity_override = severity
        fail_threshold: str | None = None

        if isinstance(policy, str):
            severity_override = policy
            fail_threshold = policy
        elif isinstance(policy, dict):
            if "severity" in policy:
                severity_override = str(policy["severity"])
            candidate = policy.get("fail_severity") or policy.get("raise_on")
            if isinstance(candidate, str):
                fail_threshold = candidate

        return severity_override, fail_threshold

    def _validate_with_schema(
        self,
        df: pd.DataFrame,
        schema: Any,
        dataset_name: str,
        *,
        severity: str = "error",
        on_error: Callable[[SchemaErrors], None] | None = None,
    ) -> pd.DataFrame:
        """Validate a dataframe against the provided Pandera schema."""

        validation_summary = self.qc_summary_data.setdefault("validation", {})

        if df is None or (hasattr(df, "empty") and df.empty):
            validation_summary[dataset_name] = {"status": "skipped", "rows": 0}
            self.record_validation_issue(
                {
                    "metric": f"schema.{dataset_name}",
                    "issue_type": "schema_validation",
                    "severity": "info",
                    "status": "skipped",
                    "rows": 0,
                }
            )
            return df

        severity_override, fail_threshold = self._resolve_schema_policy(
            dataset_name, severity=severity
        )

        try:
            validated = schema.validate(df, lazy=True)
        except SchemaErrors as exc:
            if on_error is not None:
                try:
                    on_error(exc)
                except Exception:  # pragma: no cover - defensive hook guard
                    logger.exception("schema_validation_error_hook_failed", dataset=dataset_name)
            failure_cases = getattr(exc, "failure_cases", None)
            error_count: int | None = None
            if failure_cases is not None and hasattr(failure_cases, "shape"):
                try:
                    error_count = int(failure_cases.shape[0])
                except (TypeError, ValueError):
                    error_count = None

            issue_payload: dict[str, Any] = {
                "metric": f"schema.{dataset_name}",
                "issue_type": "schema_validation",
                "severity": severity_override,
                "status": "failed",
                "errors": error_count,
            }
            if fail_threshold:
                issue_payload["fail_severity"] = fail_threshold
            if failure_cases is not None:
                try:
                    issue_payload["examples"] = failure_cases.head(5).to_dict("records")
                except Exception:  # pragma: no cover - defensive guard
                    issue_payload["examples"] = "unavailable"

            self.record_validation_issue(issue_payload)
            validation_summary[dataset_name] = {"status": "failed", "errors": error_count}
            logger.error(
                "schema_validation_failed",
                dataset=dataset_name,
                errors=error_count,
                error=str(exc),
            )
            if self._should_fail(severity_override, threshold=fail_threshold):
                raise
            return df

        validation_summary[dataset_name] = {
            "status": "passed",
            "rows": int(len(validated)),
        }
        self.record_validation_issue(
            {
                "metric": f"schema.{dataset_name}",
                "issue_type": "schema_validation",
                "severity": "info",
                "status": "passed",
                "rows": int(len(validated)),
            }
        )

        return validated  # type: ignore[no-any-return]

    def export(
        self,
        df: pd.DataFrame,
        output_path: Path,
        extended: bool = False,
    ) -> OutputArtifacts:
        """Экспортирует данные с QC отчетами."""
        logger.info("exporting_data", path=output_path, rows=len(df))
        return self.output_writer.write(
            df,
            output_path,
            metadata=self.export_metadata,
            extended=extended,
            issues=self.validation_issues,
            qc_metrics=self.qc_metrics,
            qc_summary=self.qc_summary_data,
            qc_missing_mappings=self.qc_missing_mappings,
            qc_enrichment_metrics=self.qc_enrichment_metrics,
            additional_tables=self.additional_tables,
            runtime_options=self.runtime_options,
            debug_dataset=self.debug_dataset_path,
        )

    def add_additional_table(
        self,
        name: str,
        frame: pd.DataFrame | None,
        *,
        relative_path: Path | str | None = None,
    ) -> None:
        """Register or remove an additional dataset for export."""

        if frame is None or frame.empty:
            self.additional_tables.pop(name, None)
            return

        path_value: Path | None = None
        if relative_path is not None:
            candidate = Path(relative_path)
            if candidate.is_absolute():
                logger.warning(
                    "additional_table_absolute_path_ignored",
                    name=name,
                    path=str(candidate),
                )
            else:
                path_value = candidate

        self.additional_tables[name] = AdditionalTableSpec(
            dataframe=frame,
            relative_path=path_value,
        )

    def remove_additional_table(self, name: str) -> None:
        """Remove a previously registered additional dataset."""

        self.additional_tables.pop(name, None)

    def _should_emit_debug_artifacts(self) -> bool:
        """Determine whether verbose/debug outputs should be materialised."""

        cli_options = getattr(self.config, "cli", None)
        verbose = False
        debug = False
        debug_mode = False

        if isinstance(cli_options, dict):
            verbose = bool(cli_options.get("verbose"))
            debug = bool(cli_options.get("debug"))
            mode = cli_options.get("mode")
            if isinstance(mode, str):
                debug_mode = mode.lower() == "debug"

        runtime_verbose = bool(self.runtime_options.get("verbose"))
        runtime_debug = bool(self.runtime_options.get("debug"))

        return verbose or debug or runtime_verbose or runtime_debug or debug_mode

    def _dump_debug_output(self, df: pd.DataFrame, output_path: Path) -> Path | None:
        """Persist the final dataframe as JSON when debug artefacts are enabled."""

        if not self._should_emit_debug_artifacts():
            return None

        dataset_path = output_path
        if dataset_path.suffix != ".csv":
            dataset_path = dataset_path.with_suffix(".csv")

        json_path = dataset_path.with_suffix(".json")

        try:
            self.output_writer.write_dataframe_json(df, json_path)
        except Exception as exc:  # noqa: BLE001 - surface context in logs
            logger.warning(
                "debug_dataset_write_failed",
                path=str(json_path),
                error=str(exc),
            )
            return None

        return json_path

    def run(
        self, output_path: Path, extended: bool = False, *args: Any, **kwargs: Any
    ) -> OutputArtifacts:
        """Запускает полный пайплайн: extract → transform → validate → export."""
        logger.info("pipeline_started", pipeline=self.config.pipeline.name)

        try:
            self.additional_tables = {}
            self.export_metadata = None
            self.debug_dataset_path = None
            # Extract
            df = self.extract(*args, **kwargs)
            logger.info("extraction_completed", rows=len(df))

            # Transform
            df = self.transform(df)
            logger.info("transformation_completed", rows=len(df))

            # Validate
            df = self.validate(df)
            logger.info("validation_completed", rows=len(df))

            self.debug_dataset_path = self._dump_debug_output(df, output_path)

            # Export
            artifacts = self.export(df, output_path, extended=extended)
            logger.info("pipeline_completed", artifacts=str(artifacts.dataset))

            return artifacts

        except Exception as e:
            logger.error("pipeline_failed", error=str(e))
            raise

