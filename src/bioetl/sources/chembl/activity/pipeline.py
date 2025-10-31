"""Activity Pipeline - ChEMBL activity data extraction."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import ActivitySchema
from bioetl.schemas.registry import schema_registry
from bioetl.utils.dataframe import resolve_schema_column_order
from bioetl.utils.dtypes import coerce_nullable_float, coerce_nullable_int, coerce_retry_after
from bioetl.utils.fallback import FallbackRecordBuilder, build_fallback_payload
from .client.activity_client import ActivityChEMBLClient
from .normalizer.activity_normalizer import ActivityNormalizer
from .output.activity_output import ActivityOutputWriter
from .parser.activity_parser import (
    ACTIVITY_FALLBACK_BUSINESS_COLUMNS,
    ActivityParser,
)
from .request.activity_request import ActivityRequestBuilder

schema_registry.register("activity", "1.0.0", ActivitySchema)  # type: ignore[arg-type]

__all__ = ["ActivityPipeline"]

logger = UnifiedLogger.get(__name__)

INTEGER_COLUMNS: tuple[str, ...] = (
    "standard_flag",
    "potential_duplicate",
    "src_id",
    "target_tax_id",
    "assay_id",
)
INTEGER_COLUMNS_WITH_ID: tuple[str, ...] = ("activity_id",) + INTEGER_COLUMNS
FLOAT_COLUMNS: tuple[str, ...] = ("fallback_retry_after_sec",)


@lru_cache(maxsize=1)
def _get_activity_column_order() -> list[str]:
    """Return the canonical Activity schema column order with robust fallbacks."""

    columns = resolve_schema_column_order(ActivitySchema)
    if not columns:
        try:
            columns = ActivitySchema.get_column_order()
        except AttributeError:  # pragma: no cover - defensive safeguard
            columns = []
    return list(columns)
class ActivityPipeline(PipelineBase):  # type: ignore[misc]
    """Pipeline for extracting ChEMBL activity data."""

    def __init__(
        self,
        config: PipelineConfig,
        run_id: str,
        *,
        client: ActivityChEMBLClient | None = None,
        parser: ActivityParser | None = None,
        normalizer: ActivityNormalizer | None = None,
        request_builder: ActivityRequestBuilder | None = None,
        output_writer: ActivityOutputWriter | None = None,
    ) -> None:
        super().__init__(config, run_id)
        self.primary_schema = ActivitySchema
        self._last_validation_report: dict[str, Any] | None = None
        self._fallback_stats: dict[str, Any] = {}

        self.normalizer = normalizer or ActivityNormalizer()
        self.parser = parser or ActivityParser(normalizer=self.normalizer)
        self.client = client or ActivityChEMBLClient(
            self,
            parser=self.parser,
            request_builder=request_builder,
        )
        self.api_client = self.client.api_client
        self.batch_size = self.client.batch_size
        self.configured_max_url_length = self.client.max_url_length

        self.output_writer = output_writer or ActivityOutputWriter(pipeline=self)

        self._status_snapshot: dict[str, Any] | None = None
        self._chembl_release = self._get_chembl_release()
        self.parser.set_chembl_release(self._chembl_release)
        self.client.set_release(self._chembl_release)

        self._fallback_builder = FallbackRecordBuilder(
            business_columns=ACTIVITY_FALLBACK_BUSINESS_COLUMNS,
            context={"chembl_release": self._chembl_release},
        )
        self.client.set_fallback_factory(self._create_fallback_record)

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Extract activity data from input file."""
        expected_cols = ActivitySchema.get_column_order() or []
        df, resolved_path = self.read_input_table(
            default_filename=Path("activity.csv"),
            expected_columns=expected_cols,
            input_file=input_file,
        )

        if not resolved_path.exists():
            return df

        # Map activity_chembl_id to activity_id if needed
        if 'activity_chembl_id' in df.columns:
            df = df.rename(columns={'activity_chembl_id': 'activity_id'})
            df['activity_id'] = pd.to_numeric(df['activity_id'], errors='coerce').astype('Int64')

        # Extract activity IDs for API call
        activity_ids = df['activity_id'].dropna().astype(int).unique().tolist()

        limit_value = self.get_runtime_limit()
        if limit_value is not None and len(activity_ids) > limit_value:
            logger.info(
                "applying_input_limit",
                limit=limit_value,
                original_ids=len(activity_ids),
            )
            activity_ids = activity_ids[:limit_value]

        if not activity_ids:
            logger.warning("no_activity_ids_found")
            expected_cols = ActivitySchema.get_column_order()
            return pd.DataFrame(columns=expected_cols if expected_cols else [])

        logger.info("fetching_from_chembl_api", count=len(activity_ids))
        extracted_df = self.client.extract(
            activity_ids,
            expected_columns=_get_activity_column_order(),
            integer_columns=INTEGER_COLUMNS_WITH_ID,
        )

        if extracted_df.empty:
            logger.warning("no_api_data_returned")
            expected_cols = ActivitySchema.get_column_order()
            return pd.DataFrame(columns=expected_cols if expected_cols else [])

        logger.info("extraction_completed", rows=len(extracted_df), columns=len(extracted_df.columns))
        return extracted_df

    def _create_fallback_record(
        self,
        activity_id: int,
        reason: str,
        error: Exception | None = None,
    ) -> dict[str, Any]:
        """Create deterministic fallback record enriched with error metadata."""

        record = cast(
            dict[str, Any],
            self._fallback_builder.record(
            overrides={
                "activity_id": activity_id,
                "published_relation": "=",
                "standard_relation": "=",
                "standard_units": "nM",
                "is_citation": False,
                "high_citation_rate": False,
                "exact_data_citation": False,
                "rounded_data_citation": False,
                "extracted_at": datetime.now(timezone.utc).isoformat(),
                },
            ),
        )
        metadata = build_fallback_payload(
            entity="activity",
            reason=reason,
            error=error,
            source="ChEMBL_FALLBACK",
            context=self._fallback_builder.context,
        )
        record.update(metadata)

        return record

    def _get_chembl_release(self) -> str | None:
        """Get ChEMBL database release version from the status endpoint."""

        release = self._fetch_chembl_release_info(self.api_client)
        status = release.status
        if isinstance(status, Mapping):
            self._status_snapshot = dict(status) if not isinstance(status, dict) else status
        else:
            self._status_snapshot = None

        return cast(str | None, release.version)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform activity data."""
        if df.empty:
            return df

        df = df.copy()

        pipeline_version = self.config.pipeline.version
        default_source = "chembl"

        if "source_system" in df.columns:
            df["source_system"] = df["source_system"].fillna(default_source)
        else:
            df["source_system"] = default_source

        release_value: str | None = self._chembl_release
        if isinstance(release_value, str):
            release_value = release_value.strip() or None

        coerce_nullable_int(df, INTEGER_COLUMNS_WITH_ID)

        df = self.finalize_with_standard_metadata(
            df,
            business_key="activity_id",
            sort_by=["activity_id", "source_system"],
            ascending=[True, True],
            schema=ActivitySchema,
            default_source=default_source,
            chembl_release=release_value,
        )

        self.set_export_metadata_from_dataframe(
            df,
            pipeline_version=pipeline_version,
            source_system=default_source,
            chembl_release=release_value,
        )

        self._fallback_stats = self.output_writer.capture_fallbacks(df)

        coerce_nullable_int(df, INTEGER_COLUMNS_WITH_ID)

        df = df.convert_dtypes()
        coerce_retry_after(df)

        coerce_nullable_float(df, FLOAT_COLUMNS)

        if "is_censored" in df.columns:
            df["is_censored"] = df["is_censored"].astype("boolean")

        for column in INTEGER_COLUMNS:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")

        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate activity data against schema."""
        self.validation_issues.clear()

        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            self._last_validation_report = {
                "metrics": {},
                "schema_validation": {"status": "skipped"},
                "issues": [],
            }
            return df

        df = df.copy()

        if "run_id" not in df.columns:
            df["run_id"] = self.run_id
        else:
            df["run_id"] = df["run_id"].fillna(self.run_id)

        expected_columns = _get_activity_column_order()
        if expected_columns:
            missing_columns = [column for column in expected_columns if column not in df.columns]
            if missing_columns:
                for column in missing_columns:
                    if column in INTEGER_COLUMNS_WITH_ID:
                        df[column] = pd.Series(pd.NA, index=df.index, dtype="Int64")
                    elif column in FLOAT_COLUMNS:
                        df[column] = pd.Series(np.nan, index=df.index, dtype="float64")
                    else:
                        df[column] = pd.Series(pd.NA, index=df.index)
                logger.debug(
                    "validation_missing_columns_filled",
                    columns=missing_columns,
                )

            extra_columns = [column for column in df.columns if column not in expected_columns]
            ordered_columns = list(expected_columns) + extra_columns
            if list(df.columns) != ordered_columns:
                logger.debug(
                    "validation_reordered_columns",
                    expected=len(expected_columns),
                    extras=extra_columns,
                )
                df = df[ordered_columns]

        coerce_retry_after(df)
        coerce_nullable_int(df, INTEGER_COLUMNS_WITH_ID)
        coerce_nullable_float(df, FLOAT_COLUMNS)

        qc_metrics = self._calculate_qc_metrics(df)
        fallback_stats = getattr(self, "_fallback_stats", None) or {}

        if fallback_stats:
            thresholds = getattr(self.config.qc, "thresholds", {}) or {}

            raw_count_threshold = thresholds.get("fallback.count")
            try:
                count_threshold = int(raw_count_threshold) if raw_count_threshold is not None else int(
                    fallback_stats.get("total_rows", len(df))
                )
            except (TypeError, ValueError):
                count_threshold = int(fallback_stats.get("total_rows", len(df)))

            raw_rate_threshold = thresholds.get("fallback.rate")
            try:
                rate_threshold = float(raw_rate_threshold) if raw_rate_threshold is not None else 1.0
            except (TypeError, ValueError):
                rate_threshold = 1.0

            fallback_count = int(fallback_stats.get("fallback_count", 0))
            fallback_rate = float(fallback_stats.get("fallback_rate", 0.0))

            count_severity = "info"
            if fallback_count > count_threshold:
                count_severity = "error"
            elif fallback_count > 0:
                count_severity = "warning"

            rate_severity = "info"
            if fallback_rate > rate_threshold:
                rate_severity = "error"
            elif fallback_rate > 0:
                rate_severity = "warning"

            qc_metrics["fallback.count"] = {
                "value": fallback_count,
                "threshold": count_threshold,
                "passed": fallback_count <= count_threshold,
                "severity": count_severity,
                "details": {
                    "activity_ids": fallback_stats.get("activity_ids", []),
                    "reason_counts": fallback_stats.get("reason_counts", {}),
                },
            }

            qc_metrics["fallback.rate"] = {
                "value": fallback_rate,
                "threshold": rate_threshold,
                "passed": fallback_rate <= rate_threshold,
                "severity": rate_severity,
                "details": {
                    "fallback_count": fallback_count,
                    "total_rows": fallback_stats.get("total_rows", len(df)),
                },
            }

            self.qc_summary_data.setdefault("metrics", {}).update(
                {
                    "fallback.count": qc_metrics["fallback.count"],
                    "fallback.rate": qc_metrics["fallback.rate"],
                }
            )

        self.set_qc_metrics(qc_metrics)
        self._last_validation_report = {"metrics": qc_metrics}

        failing_metrics: dict[str, Any] = {}
        for metric_name, metric in qc_metrics.items():
            log_method = logger.error if not metric["passed"] else logger.info
            log_method(
                "qc_metric",
                metric=metric_name,
                value=metric["value"],
                threshold=metric["threshold"],
                severity=metric["severity"],
                details=metric.get("details"),
            )

            issue_payload: dict[str, Any] = {
                "metric": f"qc.{metric_name}",
                "issue_type": "qc_metric",
                "severity": metric.get("severity", "info"),
                "value": metric.get("value"),
                "threshold": metric.get("threshold"),
                "passed": metric.get("passed"),
            }
            if metric.get("details"):
                issue_payload["details"] = metric["details"]
            self.record_validation_issue(issue_payload)

            if (not metric["passed"]) and self._should_fail(metric.get("severity", "info")):
                failing_metrics[metric_name] = metric

        if failing_metrics:
            self._last_validation_report["failing_metrics"] = failing_metrics
            if self._last_validation_report is not None:
                self._last_validation_report["issues"] = list(self.validation_issues)
            logger.error("qc_threshold_exceeded", failing_metrics=failing_metrics)
            raise ValueError("QC thresholds exceeded for metrics: " + ", ".join(failing_metrics.keys()))

        severity_threshold = self.config.qc.severity_threshold
        failure_report: dict[str, Any] | None = None

        def _handle_schema_failure(exc: Exception, should_fail: bool) -> None:
            nonlocal failure_report

            failure_cases = getattr(exc, "failure_cases", None)
            error_count: int | None = None
            if failure_cases is not None and hasattr(failure_cases, "shape"):
                try:
                    error_count = int(failure_cases.shape[0])
                except (TypeError, ValueError):
                    error_count = None

            if isinstance(failure_cases, pd.DataFrame) and not failure_cases.empty:
                try:
                    grouped = failure_cases.groupby("column", dropna=False)
                    for column, group in grouped:
                        column_name = (
                            str(column)
                            if column is not None
                            and not (isinstance(column, float) and pd.isna(column))
                            else "<dataframe>"
                        )
                        issue_details: dict[str, Any] = {
                            "metric": "schema.validation",
                            "issue_type": "schema_validation",
                            "severity": "critical",
                            "column": column_name,
                            "check": ", ".join(
                                sorted({str(check) for check in group["check"].dropna().unique()})
                            )
                            or "<unspecified>",
                            "count": int(group.shape[0]),
                        }
                        failure_examples = (
                            group["failure_case"].dropna().astype(str).unique().tolist()[:5]
                        )
                        if failure_examples:
                            issue_details["examples"] = failure_examples
                        self.record_validation_issue(issue_details)
                except Exception:  # pragma: no cover - defensive grouping fallback
                    self.record_validation_issue(
                        {
                            "metric": "schema.validation",
                            "issue_type": "schema_validation",
                            "severity": "critical",
                            "details": "failed_to_group_failure_cases",
                        }
                    )

            failure_report = {
                "status": "failed",
                "errors": error_count,
                "failure_cases": failure_cases,
                "should_fail": should_fail,
            }

            if not should_fail:
                logger.warning(
                    "schema_validation_below_threshold",
                    severity_threshold=severity_threshold,
                )

        def _handle_schema_success(validated: pd.DataFrame) -> None:
            if self._last_validation_report is not None:
                self._last_validation_report["schema_validation"] = {
                    "status": "passed",
                    "errors": 0,
                }
                self._last_validation_report["issues"] = list(self.validation_issues)

        try:
            validated_df = self._validate_with_schema(
                df,
                ActivitySchema,
                dataset_name="activity",
                severity="critical",
                metric_name="schema.validation",
                failure_handler=_handle_schema_failure,
                success_handler=_handle_schema_success,
            )
        except Exception:
            if self._last_validation_report is not None and failure_report is not None:
                self._last_validation_report["schema_validation"] = {
                    "status": "failed",
                    "errors": failure_report.get("errors"),
                    "failure_cases": failure_report.get("failure_cases"),
                }
                self._last_validation_report["issues"] = list(self.validation_issues)
            raise

        if failure_report and not failure_report.get("should_fail", False):
            if self._last_validation_report is not None:
                self._last_validation_report["schema_validation"] = {
                    "status": "failed",
                    "errors": failure_report.get("errors"),
                    "failure_cases": failure_report.get("failure_cases"),
                }
                self._last_validation_report["issues"] = list(self.validation_issues)
            return validated_df

        logger.info("schema_validation_passed", rows=len(validated_df))
        self.refresh_validation_issue_summary()
        return validated_df

    def close_resources(self) -> None:
        """Закрытие дополнительных ресурсов (отсутствуют для Activity)."""
        # Здесь нет дополнительных ресурсов кроме зарегистрированных API‑клиентов.
        # Базовый ``close`` их закроет через ``register_client``.
        return None

    @property
    def last_validation_report(self) -> dict[str, Any] | None:
        """Return the most recent validation report."""
        return self._last_validation_report

    def _calculate_qc_metrics(self, df: pd.DataFrame) -> dict[str, Any]:
        """Compute QC metrics for validation."""
        qc_config = self.config.qc
        thresholds = qc_config.thresholds or {}

        metrics: dict[str, Any] = {}

        duplicate_threshold = thresholds.get("duplicates")
        duplicate_config = getattr(qc_config, "duplicate_check", None)
        duplicate_field = "activity_id"
        if duplicate_config and isinstance(duplicate_config, dict):
            duplicate_field = duplicate_config.get("field", duplicate_field)
            if duplicate_threshold is None:
                duplicate_threshold = duplicate_config.get("threshold")
        if duplicate_threshold is None:
            duplicate_threshold = 0

        duplicate_count = 0
        duplicate_values: list[Any] = []
        if duplicate_field in df.columns:
            duplicate_mask = df[duplicate_field].duplicated(keep=False)
            duplicate_count = int(df[duplicate_field].duplicated().sum())
            duplicate_values = df.loc[duplicate_mask, duplicate_field].tolist()

        metrics["duplicates"] = {
            "value": duplicate_count,
            "threshold": duplicate_threshold,
            "passed": duplicate_count <= duplicate_threshold,
            "severity": "error" if duplicate_count > duplicate_threshold else "info",
            "details": {"field": duplicate_field, "duplicate_values": duplicate_values},
        }

        critical_columns = ["standard_value", "standard_type", "molecule_chembl_id"]
        null_rates: dict[str, float] = {}
        column_thresholds: dict[str, float] = {}

        def _coerce_threshold(value: Any | None, default: float) -> float:
            """Return a float threshold, preserving explicit zero values."""

            if value is None:
                return float(default)
            try:
                return float(value)
            except (TypeError, ValueError):
                return float(default)

        null_rate_threshold = _coerce_threshold(thresholds.get("null_rate_critical"), 1.0)
        null_threshold_default = _coerce_threshold(
            thresholds.get("activity.null_fraction"), null_rate_threshold
        )

        for column in critical_columns:
            column_present = column in df.columns
            if column_present and len(df) > 0:
                null_fraction = float(df[column].isna().mean())
                null_count = int(df[column].isna().sum())
            elif column_present:
                null_fraction = 0.0
                null_count = 0
            else:
                null_fraction = 1.0
                null_count = len(df)

            column_threshold = _coerce_threshold(
                thresholds.get(f"activity.null_fraction.{column}"), null_threshold_default
            )

            null_rates[column] = null_fraction
            column_thresholds[column] = column_threshold

            metrics[f"null_fraction.{column}"] = {
                "value": null_fraction,
                "threshold": column_threshold,
                "passed": null_fraction <= column_threshold,
                "severity": "error" if null_fraction > column_threshold else "info",
                "details": {
                    "column": column,
                    "column_present": column_present,
                    "null_count": null_count,
                },
            }

        max_null_rate = max(null_rates.values()) if null_rates else 0.0
        metrics["null_rate"] = {
            "value": max_null_rate,
            "threshold": null_rate_threshold,
            "passed": max_null_rate <= null_rate_threshold,
            "severity": "error" if max_null_rate > null_rate_threshold else "info",
            "details": {
                "column_null_rates": null_rates,
                "column_thresholds": column_thresholds,
            },
        }

        invalid_units_threshold = float(thresholds.get("invalid_units", 0))
        invalid_units_count = 0
        invalid_indices: list[int] = []
        invalid_fraction = 0.0
        if {"standard_value", "standard_units"}.issubset(df.columns) and len(df) > 0:
            mask = df["standard_value"].notna() & df["standard_units"].isna()
            invalid_units_count = int(mask.sum())
            invalid_indices = df.index[mask].tolist()
            invalid_fraction = invalid_units_count / len(df)

        metrics["invalid_units"] = {
            "value": invalid_units_count,
            "threshold": invalid_units_threshold,
            "passed": invalid_units_count <= invalid_units_threshold,
            "severity": "error" if invalid_units_count > invalid_units_threshold else "info",
            "details": {"invalid_row_indices": invalid_indices, "invalid_fraction": invalid_fraction},
        }

        return metrics

