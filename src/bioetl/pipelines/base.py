"""Base pipeline class."""

from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import Any

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
from bioetl.utils.qc import QCMetricsRegistry, update_summary_metrics

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

    def _should_fail(self, severity: str) -> bool:
        """Determine if the given severity breaches the configured threshold."""

        threshold = self.config.qc.severity_threshold.lower()
        return self._severity_value(severity) >= self._severity_value(threshold)

    def _should_fail_for_dataset(self, severity: str, *, fail_on: str | None = None) -> bool:
        """Determine if a dataset-level severity should result in failure."""

        if fail_on is not None:
            return self._severity_value(severity) >= self._severity_value(fail_on)
        return self._should_fail(severity)

    def _resolve_validation_rule(self, dataset_name: str) -> tuple[str, str | None]:
        """Fetch schema validation overrides for the dataset."""

        validation_config = getattr(self.config.qc, "validation", {}) or {}
        rule = validation_config.get(dataset_name)
        if rule is None:
            return "error", None

        severity = getattr(rule, "severity", None) or "error"
        fail_on = getattr(rule, "fail_on", None)
        if isinstance(fail_on, str):
            fail_on = fail_on.lower()
        return str(severity), fail_on

    def finalize_qc_metrics(
        self,
        registry: QCMetricsRegistry,
        *,
        prefix: str = "qc",
        raise_on_failure: bool = True,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        """Persist QC metrics, emit issues, and optionally fail the pipeline."""

        metrics_payload = registry.as_dict()
        failing: dict[str, dict[str, Any]] = {}

        for name, metric in registry.items():
            payload = metric.to_payload()
            log_method = logger.error if not metric.passed else logger.info
            log_method(
                "qc_metric",
                metric=name,
                value=payload.get("value"),
                severity=metric.severity,
                threshold=payload.get("threshold"),
                threshold_min=metric.threshold_min,
                threshold_max=metric.threshold_max,
                details=metric.details,
            )

            self.record_validation_issue(metric.to_issue_payload(prefix=prefix))

            if (not metric.passed) and self._should_fail(metric.severity):
                failing[name] = payload

        self.qc_metrics = metrics_payload
        update_summary_metrics(self.qc_summary_data, metrics_payload)

        if raise_on_failure and failing:
            logger.error("qc_threshold_exceeded", failing=failing)
            raise ValueError(
                "QC thresholds exceeded for metrics: " + ", ".join(sorted(failing.keys()))
            )

        return metrics_payload, failing

    def _validate_with_schema(
        self,
        df: pd.DataFrame,
        schema: Any,
        dataset_name: str,
        *,
        severity: str | None = None,
        issue_metric: str | None = None,
        on_failure: Callable[[SchemaErrors, dict[str, Any]], None] | None = None,
    ) -> pd.DataFrame:
        """Validate a dataframe against the provided schema with standardised QC logging."""

        validation_summary = self.qc_summary_data.setdefault("validation", {})
        metric_name = issue_metric or f"schema.{dataset_name}"

        resolved_severity, fail_on = self._resolve_validation_rule(dataset_name)
        effective_severity = severity or resolved_severity

        if df is None or (hasattr(df, "empty") and df.empty):
            validation_summary[dataset_name] = {"status": "skipped", "rows": 0}
            self.record_validation_issue(
                {
                    "metric": metric_name,
                    "issue_type": "schema_validation",
                    "severity": "info",
                    "status": "skipped",
                    "rows": 0,
                }
            )
            return df

        try:
            validated = schema.validate(df, lazy=True)
        except SchemaErrors as exc:
            failure_cases = getattr(exc, "failure_cases", None)
            error_count: int | None = None
            if failure_cases is not None and hasattr(failure_cases, "shape"):
                try:
                    error_count = int(failure_cases.shape[0])
                except (TypeError, ValueError):
                    error_count = None

            issue_payload: dict[str, Any] = {
                "metric": metric_name,
                "issue_type": "schema_validation",
                "severity": str(effective_severity),
                "status": "failed",
                "errors": error_count,
            }

            if failure_cases is not None:
                try:
                    issue_payload["examples"] = failure_cases.head(5).to_dict("records")
                except Exception:  # pragma: no cover - defensive guard
                    issue_payload["examples"] = "unavailable"

            if on_failure is not None:
                on_failure(exc, issue_payload)

            self.record_validation_issue(issue_payload)
            validation_summary[dataset_name] = {"status": "failed", "errors": error_count}
            logger.error(
                "schema_validation_failed",
                dataset=dataset_name,
                errors=error_count,
                error=str(exc),
            )

            if self._should_fail_for_dataset(effective_severity, fail_on=fail_on):
                raise
            return df

        validation_summary[dataset_name] = {"status": "passed", "rows": int(len(validated))}
        self.record_validation_issue(
            {
                "metric": metric_name,
                "issue_type": "schema_validation",
                "severity": "info",
                "status": "passed",
                "rows": int(len(validated)),
            }
        )

        return validated  # type: ignore[no-any-return]

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

