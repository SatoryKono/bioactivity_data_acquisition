"""Base pipeline class and enrichment stage registry helpers."""

from __future__ import annotations

import abc
from abc import ABC, abstractmethod
from datetime import datetime
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger
from bioetl.core.output_writer import (
    AdditionalTableSpec,
    OutputArtifacts,
    OutputMetadata,
)
from bioetl.core.chembl import (
    ChemblClientContext,
    build_chembl_client_context as _core_build_chembl_client_context,
    create_chembl_client as _core_create_chembl_client,
    create_pipeline_output_writer,
)
from bioetl.core.unified_schema import get_schema, get_schema_metadata
from bioetl.utils.chembl import ChemblRelease, SupportsRequestJson, fetch_chembl_release
from bioetl.utils.io import load_input_frame, resolve_input_path
from bioetl.utils.output import finalize_output_dataset
from bioetl.utils.qc import (
    update_summary_metrics,
    update_summary_section,
    update_validation_issue_summary,
)
from bioetl.utils.validation import _summarize_schema_errors

logger = UnifiedLogger.get(__name__)


PredicateResult = bool | tuple[bool, str | None]


build_chembl_client_context = _core_build_chembl_client_context
create_chembl_client = _core_create_chembl_client
_build_chembl_client_context = build_chembl_client_context


@dataclass(frozen=True)
class EnrichmentStage:
    """Definition of an enrichment stage executed during transformation."""

    name: str
    include_if: Callable[[PipelineBase, pd.DataFrame], PredicateResult]
    handler: Callable[[PipelineBase, pd.DataFrame], pd.DataFrame | None]

    def should_run(self, pipeline: PipelineBase, df: pd.DataFrame) -> tuple[bool, str | None]:
        """Evaluate the inclusion predicate and normalise the response."""

        result = self.include_if(pipeline, df)
        if isinstance(result, tuple):
            include, reason = result
        else:
            include, reason = bool(result), None
        return bool(include), reason

    def execute(self, pipeline: PipelineBase, df: pd.DataFrame) -> pd.DataFrame | None:
        """Invoke the stage handler."""

        return self.handler(pipeline, df)


class EnrichmentStageRegistry:
    """Global registry storing enrichment stages per pipeline class."""

    def __init__(self) -> None:
        self._registry: dict[type[PipelineBase], list[EnrichmentStage]] = {}

    def register(self, pipeline_cls: type[PipelineBase], stage: EnrichmentStage) -> None:
        """Register or replace an enrichment stage for the given pipeline class."""

        stages = self._registry.setdefault(pipeline_cls, [])
        for index, existing in enumerate(stages):
            if existing.name == stage.name:
                stages[index] = stage
                break
        else:
            stages.append(stage)

    def get(self, pipeline_cls: type[PipelineBase]) -> Iterable[EnrichmentStage]:
        """Return the registered stages for the pipeline class."""

        return tuple(self._registry.get(pipeline_cls, ()))


enrichment_stage_registry = EnrichmentStageRegistry()


class PipelineBase(ABC):
    """Базовый класс для всех пайплайнов.

    Реализации ``extract`` должны вызывать :meth:`read_input_table` для чтения
    входных CSV. Хелпер централизует логирование, обработку ``limit`` и
    отсутствие файлов, поэтому новые пайплайны автоматически наследуют единое
    поведение.
    """

    def __init__(self, config: PipelineConfig, run_id: str):
        self.config = config
        self.run_id = run_id
        self.determinism = config.determinism
        self.output_writer = create_pipeline_output_writer(
            run_id,
            determinism=self.determinism,
            pipeline_config=config,
        )
        self.primary_schema: Any | None = None
        self.validation_issues: list[dict[str, Any]] = []
        self.qc_metrics: dict[str, Any] = {}
        self.qc_summary_data: dict[str, Any] = {}
        self.qc_missing_mappings = pd.DataFrame()
        self.qc_enrichment_metrics = pd.DataFrame()
        self.runtime_options: dict[str, Any] = {}
        self.additional_tables: dict[str, AdditionalTableSpec] = {}
        self.export_metadata: OutputMetadata | None = None
        self.debug_dataset_path: Path | None = None
        self.stage_context: dict[str, Any] = {}
        self._clients: list[UnifiedAPIClient] = []
        logger.info("pipeline_initialized", pipeline=config.pipeline.name, run_id=run_id)

    _SEVERITY_LEVELS: dict[str, int] = {"info": 0, "warning": 1, "error": 2, "critical": 3}

    def _init_chembl_client(
        self,
        *,
        defaults: Mapping[str, Any] | None = None,
        batch_size_cap: int | None = None,
    ) -> ChemblClientContext:
        """Return a configured ChEMBL client context for the pipeline instance."""

        return build_chembl_client_context(
            self.config,
            defaults=defaults,
            batch_size_cap=batch_size_cap,
        )

    def record_validation_issue(self, issue: dict[str, Any]) -> None:
        """Store a validation issue for later QC reporting."""

        issue = issue.copy()
        issue.setdefault("metric", "validation_issue")
        issue.setdefault("severity", "info")
        self.validation_issues.append(issue)

    def reset_stage_context(self) -> None:
        """Clear any data cached by enrichment stages."""

        self.stage_context.clear()

    def reset_run_state(self) -> None:
        """Reset per-run quality control tracking containers."""

        self.validation_issues.clear()
        self.qc_metrics = {}
        self.qc_summary_data = {}
        self.qc_missing_mappings = pd.DataFrame()
        self.qc_enrichment_metrics = pd.DataFrame()
        self.runtime_options.clear()
        self.reset_stage_context()

    def get_stage_summary(self, name: str) -> dict[str, Any] | None:
        """Return the summary payload for a specific stage if present."""

        stages = self.qc_summary_data.get("stages")
        if not isinstance(stages, dict):
            return None
        payload = stages.get(name)
        return payload if isinstance(payload, dict) else None

    def read_input_table(
        self,
        *,
        default_filename: str | Path,
        expected_columns: Sequence[str] | None = None,
        dtype: Any | None = None,
        input_file: Path | None = None,
        apply_limit: bool = True,
        **read_csv_kwargs: Any,
    ) -> tuple[pd.DataFrame, Path]:
        """Read an input CSV applying shared logging and runtime limits.

        Parameters
        ----------
        default_filename:
            Имя файла по умолчанию внутри ``paths.input_root``.
        expected_columns:
            Последовательность столбцов для пустого датафрейма, если файл
            отсутствует.
        dtype:
            Значение ``dtype`` для ``pandas.read_csv``.
        input_file:
            Переопределение имени входного файла, например из CLI.
        apply_limit:
            Управляет применением ``limit``/``sample`` при чтении файла.
        read_csv_kwargs:
            Дополнительные аргументы, передаваемые ``pandas.read_csv``.
        """

        input_path = Path(input_file) if input_file is not None else Path(default_filename)
        resolved_path = resolve_input_path(self.config, input_path)

        limit_value = self.get_runtime_limit() if apply_limit else None
        log_payload: dict[str, Any] = {"path": resolved_path}
        if limit_value is not None:
            log_payload["limit"] = limit_value
        logger.info("reading_input", **log_payload)

        dataframe = load_input_frame(
            self.config,
            resolved_path,
            expected_columns=expected_columns,
            limit=limit_value,
            dtype=dtype,
            **read_csv_kwargs,
        )

        if not resolved_path.exists():
            logger.warning("input_file_not_found", path=resolved_path)
            return dataframe, resolved_path

        if limit_value is not None:
            logger.info("input_limit_active", limit=limit_value, rows=len(dataframe))

        return dataframe, resolved_path

    def _fetch_chembl_release_info(
        self,
        api_client: SupportsRequestJson | str | None,
    ) -> ChemblRelease:
        """Resolve the ChEMBL release metadata and normalise logging."""

        if api_client is None:
            return ChemblRelease(version=None, status=None)

        try:
            release = fetch_chembl_release(api_client)
        except Exception as exc:  # noqa: BLE001 - upstream errors are non-fatal
            logger.warning("failed_to_get_chembl_version", error=str(exc))
            return ChemblRelease(version=None, status=None)

        status = release.status
        version = release.version.strip() if isinstance(release.version, str) else None

        if version:
            release_date = (
                status.get("chembl_release_date") if isinstance(status, Mapping) else None
            )
            activities = status.get("activities") if isinstance(status, Mapping) else None
            logger.info(
                "chembl_version_fetched",
                version=version,
                release_date=release_date,
                activities=activities,
            )
            return ChemblRelease(version=version, status=status)

        logger.warning("chembl_version_not_in_status_response")
        return ChemblRelease(version=None, status=status)

    def set_stage_summary(self, name: str, status: str, **details: Any) -> None:
        """Record the execution summary for an enrichment stage."""

        payload = {"status": status}
        payload.update({key: value for key, value in details.items() if value is not None})
        stages = self.qc_summary_data.setdefault("stages", {})
        stages[name] = payload

    def add_qc_summary_section(
        self,
        section: str,
        values: Mapping[str, Any] | Any,
        *,
        merge: bool = True,
    ) -> Any:
        """Update a QC summary section using the unified helper."""

        return update_summary_section(self.qc_summary_data, section, values, merge=merge)

    def add_qc_summary_sections(
        self, sections: Mapping[str, Mapping[str, Any]], *, merge: bool = True
    ) -> None:
        """Populate multiple QC summary sections at once."""

        for name, payload in sections.items():
            self.add_qc_summary_section(name, payload, merge=merge)

    def set_qc_metrics(self, metrics: Mapping[str, Any], *, merge: bool = False) -> None:
        """Store QC metrics and synchronise the summary payload."""

        if not merge:
            self.qc_metrics = dict(metrics)
        else:
            self.qc_metrics.update(dict(metrics))

        update_summary_metrics(self.qc_summary_data, self.qc_metrics)

    def refresh_validation_issue_summary(self) -> None:
        """Recompute validation issue counters inside the QC summary."""

        update_validation_issue_summary(self.qc_summary_data, self.validation_issues)

    def set_export_metadata(self, metadata: OutputMetadata | None) -> None:
        """Assign export metadata for downstream materialisation."""

        self.export_metadata = metadata

    def set_export_metadata_from_dataframe(
        self,
        df: pd.DataFrame,
        *,
        pipeline_version: str,
        source_system: str,
        chembl_release: str | None = None,
        column_order: list[str] | None = None,
        config_hash: str | None = None,
        git_commit: str | None = None,
        sources: Sequence[str] | None = None,

        schema: type[Any] | None = None,
        hash_policy_version: str | None = None,
    ) -> OutputMetadata:
        """Create and assign :class:`OutputMetadata` from a dataframe."""

        resolved_config_hash = config_hash
        if resolved_config_hash is None:
            resolved_config_hash = getattr(self.config, "config_hash", None)

        resolved_git_commit = git_commit
        if resolved_git_commit is None:
            resolved_git_commit = getattr(self, "git_commit", None)

        resolved_schema = schema
        if resolved_schema is None:
            candidate_schema = getattr(self, "primary_schema", None)
            if isinstance(candidate_schema, type):
                resolved_schema = candidate_schema

        resolved_sources: Sequence[str] | None = sources
        if resolved_sources is None:
            config_sources = getattr(self.config, "sources", None)
            if isinstance(config_sources, Mapping):
                enabled: list[str] = []
                for name, definition in config_sources.items():
                    if getattr(definition, "enabled", True):
                        enabled.append(str(name))
                resolved_sources = enabled

        resolved_hash_policy_version = hash_policy_version
        if resolved_hash_policy_version is None:
            determinism = getattr(self.config, "determinism", None)
            resolved_hash_policy_version = getattr(
                determinism, "hash_policy_version", None
            )
        if resolved_hash_policy_version is None:
            schema = getattr(self, "primary_schema", None)
            resolved_hash_policy_version = getattr(schema, "hash_policy_version", None)

        metadata = OutputMetadata.from_dataframe(
            df,
            pipeline_version=pipeline_version,
            source_system=source_system,
            chembl_release=chembl_release,
            column_order=column_order or list(df.columns),
            run_id=self.run_id,
            config_hash=resolved_config_hash,
            git_commit=resolved_git_commit,
            sources=resolved_sources,

            schema=resolved_schema,
            hash_policy_version=resolved_hash_policy_version,
        )

        self.set_export_metadata(metadata)
        return metadata

    def finalize_with_standard_metadata(
        self,
        df: pd.DataFrame,
        *,
        business_key: str,
        sort_by: Sequence[str] | None = None,
        ascending: Sequence[bool] | bool | None = None,
        schema: type[Any] | None = None,
        default_source: str | None,
        chembl_release: str | None,
        extracted_at: Any | None = None,
        metadata: Mapping[str, Any] | None = None,
        overwrite_metadata: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Apply canonical metadata defaults before delegating finalisation."""

        if df.empty:
            return df.copy()

        working = df.copy()

        if default_source is not None:
            if "source_system" in working.columns:
                working["source_system"] = working["source_system"].fillna(default_source)
            else:
                working["source_system"] = default_source

        resolved_release: str | None
        if isinstance(chembl_release, str):
            resolved_release = chembl_release.strip() or None
        else:
            resolved_release = chembl_release

        if "chembl_release" in working.columns:
            if resolved_release is None:
                working["chembl_release"] = working["chembl_release"].where(
                    working["chembl_release"].notna(),
                    pd.NA,
                )
            else:
                working["chembl_release"] = working["chembl_release"].fillna(resolved_release)
        else:
            if resolved_release is None:
                working["chembl_release"] = pd.Series(pd.NA, index=working.index, dtype="string")
            else:
                working["chembl_release"] = resolved_release

        if extracted_at is None:
            resolved_extracted_at = pd.Timestamp.now(tz="UTC").isoformat()
        elif isinstance(extracted_at, (pd.Timestamp, datetime)):
            resolved_extracted_at = extracted_at.isoformat()
        else:
            resolved_extracted_at = str(extracted_at)

        if "extracted_at" in working.columns:
            working["extracted_at"] = working["extracted_at"].fillna(resolved_extracted_at)
        else:
            working["extracted_at"] = resolved_extracted_at

        metadata_payload: dict[str, Any] = dict(metadata or {})
        metadata_payload.setdefault("pipeline_version", self.config.pipeline.version)
        metadata_payload.setdefault("run_id", self.run_id)

        if default_source is not None:
            metadata_payload.setdefault("source_system", default_source)
        else:
            metadata_payload.setdefault("source_system", None)

        metadata_payload.setdefault("chembl_release", resolved_release)
        metadata_payload.setdefault("extracted_at", resolved_extracted_at)

        return finalize_output_dataset(
            working,
            business_key=business_key,
            sort_by=sort_by,
            ascending=ascending,
            schema=schema,
            metadata=metadata_payload,
            overwrite_metadata=overwrite_metadata,
        )

    def execute_enrichment_stages(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute registered enrichment stages in sequence for the pipeline."""

        stages = tuple(enrichment_stage_registry.get(type(self)))
        if not stages:
            return df

        working_df = df
        for stage in stages:
            include, reason = stage.should_run(self, working_df)
            if not include:
                logger.info(
                    "enrichment_stage_skipped",
                    stage=stage.name,
                    reason=reason,
                )
                if self.get_stage_summary(stage.name) is None:
                    metadata = {"reason": reason} if reason else {}
                    self.set_stage_summary(stage.name, "skipped", **metadata)
                continue

            logger.info("enrichment_stage_started", stage=stage.name)
            try:
                result = stage.execute(self, working_df)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error(
                    "enrichment_stage_failed",
                    stage=stage.name,
                    error=str(exc),
                )
                if self.get_stage_summary(stage.name) is None:
                    self.set_stage_summary(stage.name, "failed", error=str(exc))
                continue

            if result is not None:
                working_df = result

            if self.get_stage_summary(stage.name) is None:
                self.set_stage_summary(stage.name, "completed", rows=int(len(working_df)))

            logger.info(
                "enrichment_stage_completed",
                stage=stage.name,
                rows=len(working_df),
            )

        return working_df

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

    def run_schema_validation(
        self,
        df: pd.DataFrame,
        schema: Any,
        *,
        dataset_name: str,
        severity: str = "error",
        metric_name: str | None = None,
        success_callbacks: Iterable[Callable[[pd.DataFrame], None]] | None = None,
        failure_callbacks: Iterable[Callable[[list[dict[str, Any]], Exception, bool], None]] | None = None,
        error_adapter: Callable[[list[dict[str, Any]], Exception, bool], Exception | None]
        | None = None,
    ) -> pd.DataFrame:
        """Execute Pandera validation with shared QC handling and callbacks.

        Parameters
        ----------
        df:
            DataFrame to validate.
        schema:
            Pandera schema object providing ``validate``.
        dataset_name:
            Identifier used for QC reporting.
        severity:
            Severity label forwarded to :meth:`_validate_with_schema`.
        metric_name:
            Optional explicit QC metric identifier.
        success_callbacks:
            Callbacks invoked with the validated dataframe when validation
            succeeds without issues.
        failure_callbacks:
            Callbacks invoked when validation raises an exception.  Each
            callback receives a list of schema issues, the underlying
            exception and the ``should_fail`` flag calculated by
            :meth:`_should_fail`.
        error_adapter:
            Optional callable converting schema issues into a raised exception.
            When provided and validation raises, the adapter is given the
            issues, original exception and ``should_fail`` flag.  Returning an
            exception instance causes it to be raised instead of the original
            error.
        """

        success_callbacks = tuple(success_callbacks or ())
        failure_callbacks = tuple(failure_callbacks or ())

        schema_issues: list[dict[str, Any]] = []
        should_fail_flag = False

        def _handle_schema_failure(exc: Exception, should_fail: bool) -> None:
            nonlocal schema_issues, should_fail_flag

            should_fail_flag = bool(should_fail)

            failure_cases = getattr(exc, "failure_cases", None)
            schema_issues = _summarize_schema_errors(failure_cases)

            for issue in schema_issues:
                self.record_validation_issue(issue)
                logger.error(
                    "schema_validation_error",
                    dataset=dataset_name,
                    column=issue.get("column"),
                    check=issue.get("check"),
                    count=issue.get("count"),
                    severity=issue.get("severity"),
                )

            for callback in failure_callbacks:
                try:
                    callback(schema_issues, exc, should_fail_flag)
                except Exception as callback_exc:  # pragma: no cover - defensive
                    logger.warning(
                        "qc_failure_callback_error",
                        dataset=dataset_name,
                        callback=getattr(callback, "__name__", type(callback).__name__),
                        error=str(callback_exc),
                    )

        try:
            validated_df = self._validate_with_schema(
                df,
                schema,
                dataset_name=dataset_name,
                severity=severity,
                metric_name=metric_name,
                failure_handler=_handle_schema_failure,
            )
        except Exception as exc:  # pragma: no cover - exercised via adapter tests
            if error_adapter is not None:
                try:
                    adapted_error = error_adapter(schema_issues, exc, should_fail_flag)
                except Exception as adapter_exc:  # pragma: no cover - defensive
                    logger.warning(
                        "qc_error_adapter_failed",
                        dataset=dataset_name,
                        error=str(adapter_exc),
                    )
                    adapted_error = None
                if adapted_error is not None:
                    raise adapted_error from exc
            raise

        if schema_issues:
            return validated_df

        for callback in success_callbacks:
            try:
                callback(validated_df)
            except Exception as callback_exc:  # pragma: no cover - defensive
                logger.warning(
                    "qc_success_callback_error",
                    dataset=dataset_name,
                    callback=getattr(callback, "__name__", type(callback).__name__),
                    error=str(callback_exc),
                )

        return validated_df

    def _validate_with_schema(
        self,
        df: pd.DataFrame,
        schema: Any,
        *,
        dataset_name: str,
        severity: str = "error",
        metric_name: str | None = None,
        failure_handler: Callable[[Exception, bool], None] | None = None,
        success_handler: Callable[[pd.DataFrame], None] | None = None,
    ) -> pd.DataFrame:
        """Validate a dataframe using a Pandera schema with QC reporting hooks."""

        dataset_label = str(dataset_name)
        severity_label = str(severity).lower()

        validation_summary = self.qc_summary_data.setdefault("validation", {})
        metric_label = metric_name or f"schema.{dataset_label}"

        if df is None or (hasattr(df, "empty") and df.empty):
            validation_summary[dataset_label] = {
                "status": "skipped",
                "rows": 0,
                "severity": "info",
            }
            self.record_validation_issue(
                {
                    "metric": metric_label,
                    "issue_type": "schema_validation",
                    "severity": "info",
                    "status": "skipped",
                    "rows": 0,
                }
            )
            return df

        try:
            validated = schema.validate(df, lazy=True)
        except Exception as exc:
            failure_cases = getattr(exc, "failure_cases", None)
            error_count: int | None = None
            if failure_cases is not None and hasattr(failure_cases, "shape"):
                try:
                    error_count = int(failure_cases.shape[0])
                except (TypeError, ValueError):
                    error_count = None

            should_fail = self._should_fail(severity_label)
            if failure_handler is not None:
                failure_handler(exc, should_fail)

            issue_payload: dict[str, Any] = {
                "metric": metric_label,
                "issue_type": "schema_validation",
                "severity": severity_label,
                "status": "failed",
                "errors": error_count,
            }
            if failure_cases is not None:
                try:
                    issue_payload["examples"] = failure_cases.head(5).to_dict("records")
                except Exception:  # pragma: no cover - defensive guard
                    issue_payload["examples"] = "unavailable"

            self.record_validation_issue(issue_payload)
            payload: dict[str, Any] = {"status": "failed", "severity": severity_label}
            if error_count is not None:
                payload["errors"] = error_count
            validation_summary[dataset_label] = payload

            if should_fail:
                log_method = logger.error
            elif severity_label != "info":
                log_method = logger.warning
            else:
                log_method = logger.info

            log_method(
                "schema_validation_failed",
                dataset=dataset_label,
                errors=error_count,
                error=str(exc),
            )

            if should_fail:
                raise

            return df

        validation_summary[dataset_label] = {
            "status": "passed",
            "rows": int(len(validated)),
            "severity": "info",
        }
        self.record_validation_issue(
            {
                "metric": metric_label,
                "issue_type": "schema_validation",
                "severity": "info",
                "status": "passed",
                "rows": int(len(validated)),
            }
        )

        if success_handler is not None:
            success_handler(validated)

        return validated

    @abstractmethod
    def extract(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """Извлекает данные из источника."""
        pass

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Трансформирует данные."""
        raise NotImplementedError(
            f"{type(self).__name__} must implement the transform() method"
        )

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate output data against the UnifiedSchema registry."""

        entity = getattr(self.config.pipeline, "entity", "").strip()
        if not entity:
            raise ValueError("Pipeline configuration must define pipeline.entity")

        metadata = get_schema_metadata(entity)
        if metadata is None:
            raise ValueError(f"No schema registered for entity '{entity}'")

        schema_version = metadata.version
        schema_cls = get_schema(entity, schema_version)
        self.primary_schema = schema_cls

        working_df = df.copy()

        try:
            from bioetl.utils.dataframe import resolve_schema_column_order  # noqa: PLC0415
        except Exception:  # pragma: no cover - defensive import guard
            resolve_schema_column_order = None  # type: ignore[assignment]

        canonical_order: list[str] = []
        if resolve_schema_column_order is not None:
            try:
                canonical_order = list(resolve_schema_column_order(schema_cls))
            except Exception:  # pragma: no cover - defensive guard
                canonical_order = []

        if not canonical_order:
            try:
                canonical_order = list(schema_cls.get_column_order())
            except Exception:  # pragma: no cover - defensive guard
                canonical_order = []

        if canonical_order:
            missing = [column for column in canonical_order if column not in working_df.columns]
            for column in missing:
                working_df[column] = pd.NA

            ordered_columns = [column for column in canonical_order if column in working_df.columns]
            working_df = working_df.loc[:, ordered_columns]

        validated = self.run_schema_validation(
            working_df,
            schema_cls,
            dataset_name=entity,
            severity="error",
            metric_name=f"schema.{entity}",
        )

        self.refresh_validation_issue_summary()
        return validated

    def export(
        self,
        df: pd.DataFrame,
        output_path: Path,
        extended: bool = False,
    ) -> OutputArtifacts:
        """Экспортирует данные с QC отчетами."""
        logger.info("exporting_data", path=output_path, rows=len(df))

        schema_order: list[str] | None = None
        primary_schema = getattr(self, "primary_schema", None)
        if primary_schema is not None:
            try:
                from bioetl.schemas.registry import SchemaRegistry  # noqa: PLC0415
                from bioetl.utils.dataframe import (  # noqa: PLC0415
                    resolve_schema_column_order,
                )
            except Exception:  # pragma: no cover - defensive import guard
                SchemaRegistry = None  # type: ignore[assignment]
                resolve_schema_column_order = None  # type: ignore[assignment]
            else:
                if not isinstance(primary_schema, type):
                    schema_cls = type(primary_schema)
                else:
                    schema_cls = primary_schema

                registration = SchemaRegistry.find_registration(schema_cls)
                target_schema = (
                    registration.schema if registration is not None else schema_cls
                )
                if resolve_schema_column_order is not None:
                    schema_order = resolve_schema_column_order(target_schema)

        if schema_order:
            actual_columns = list(df.columns)
            if actual_columns != schema_order:
                missing_columns = [
                    column for column in schema_order if column not in actual_columns
                ]
                unexpected_columns = [
                    column for column in actual_columns if column not in schema_order
                ]
                logger.error(
                    "export_column_order_mismatch",
                    expected=schema_order,
                    actual=actual_columns,
                    missing=missing_columns,
                    unexpected=unexpected_columns,
                )
                raise ValueError(
                    "Dataframe columns do not match SchemaRegistry order: "
                    f"expected {schema_order}, got {actual_columns}"
                )

        configured_order: list[str] = []
        sort_columns: list[str] = []
        sort_ascending: list[bool] = []
        if getattr(self, "determinism", None) is not None:
            configured_order = list(self.determinism.column_order or [])

            sort_config = getattr(self.determinism, "sort", None)
            if sort_config is not None:
                source_columns = set(df.columns)
                ascending_values = list(sort_config.ascending or [])

                for index, column_name in enumerate(list(sort_config.by or [])):
                    if column_name not in source_columns:
                        continue

                    sort_columns.append(column_name)
                    if index < len(ascending_values):
                        sort_ascending.append(bool(ascending_values[index]))
                    elif ascending_values:
                        sort_ascending.append(bool(ascending_values[-1]))
                    else:
                        sort_ascending.append(True)

        export_frame = df
        if configured_order:
            export_frame = df.copy()

            missing_columns = [
                column for column in configured_order if column not in export_frame.columns
            ]
            for column in missing_columns:
                export_frame[column] = pd.NA

            extra_columns = [
                column for column in export_frame.columns if column not in configured_order
            ]
            export_frame = export_frame[configured_order + extra_columns]

            if self.export_metadata is not None:
                self.export_metadata = replace(
                    self.export_metadata,
                    column_order=list(export_frame.columns),
                    column_count=len(export_frame.columns),
                )

        if sort_columns:
            export_frame = export_frame.sort_values(
                by=sort_columns,
                ascending=sort_ascending,
                kind="stable",
            )

        artifacts = self.output_writer.write(
            export_frame,
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

        if artifacts.metadata_model is not None:
            self.export_metadata = artifacts.metadata_model

        return artifacts

    def add_additional_table(
        self,
        name: str,
        frame: pd.DataFrame | None,
        *,
        relative_path: Path | str | None = None,
        formats: Sequence[str] | None = None,
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

        resolved_formats: Sequence[str] | None = formats
        if resolved_formats is None:
            normalised_formats: tuple[str, ...] = ("csv",)
        else:
            formatted: list[str] = []
            for fmt in resolved_formats:
                if fmt is None:
                    continue
                normalised = str(fmt).strip().lower()
                if not normalised:
                    continue
                if normalised not in formatted:
                    formatted.append(normalised)
            normalised_formats = tuple(formatted or ("csv",))

        self.additional_tables[name] = AdditionalTableSpec(
            dataframe=frame,
            relative_path=path_value,
            formats=normalised_formats,
        )

    def remove_additional_table(self, name: str) -> None:
        """Remove a previously registered additional dataset."""

        self.additional_tables.pop(name, None)

    def reset_additional_tables(self) -> None:
        """Clear all registered additional datasets."""

        self.additional_tables.clear()

    def register_client(self, client: UnifiedAPIClient | None) -> None:
        """Register an API client for automatic teardown."""

        if client is None:
            return

        for existing in self._clients:
            if existing is client:
                return

        self._clients.append(client)

    def _close_resource(self, resource: Any, *, resource_name: str) -> None:
        """Close a resource if it exposes a callable ``close`` attribute."""

        if resource is None:
            return

        close_method = getattr(resource, "close", None)
        if not callable(close_method):
            return

        try:
            close_method()
        except Exception as exc:  # noqa: BLE001 - cleanup should not fail run teardown
            logger.warning(
                "resource_close_failed",
                resource=resource_name,
                error=str(exc),
            )

    @abc.abstractmethod
    def close_resources(self) -> None:
        """Close non-API resources held by the pipeline instance."""

    def close(self) -> None:
        """Close registered API clients and any additional resources."""

        closed_ids: set[int] = set()

        def _close_client(client: UnifiedAPIClient, *, label: str) -> None:
            identifier = id(client)
            if identifier in closed_ids:
                return
            closed_ids.add(identifier)
            self._close_resource(client, resource_name=label)

        for attribute_name, attribute_value in vars(self).items():
            if isinstance(attribute_value, UnifiedAPIClient):
                _close_client(attribute_value, label=f"api_client.{attribute_name}")

        for index, client in enumerate(self._clients):
            if client is None:
                continue
            _close_client(client, label=f"api_client.registered[{index}]")

        try:
            self.close_resources()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning("pipeline_resource_cleanup_failed", error=str(exc))

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
        UnifiedLogger.set_context(
            run_id=self.run_id,
            stage="bootstrap",
            actor=type(self).__name__,
            source=self.config.pipeline.entity,
        )
        logger.info("pipeline_started", pipeline=self.config.pipeline.name)

        previous_runtime_options = dict(self.runtime_options)

        try:
            self.reset_run_state()
            self.reset_additional_tables()
            if previous_runtime_options:
                self.runtime_options.update(previous_runtime_options)
            self.export_metadata = None
            self.debug_dataset_path = None
            # Extract
            UnifiedLogger.set_context(stage="extract")
            df = self.extract(*args, **kwargs)
            logger.info("extraction_completed", rows=len(df))

            # Transform
            UnifiedLogger.set_context(stage="transform")
            df = self.transform(df)
            logger.info("transformation_completed", rows=len(df))

            # Validate
            UnifiedLogger.set_context(stage="validate")
            df = self.validate(df)
            logger.info("validation_completed", rows=len(df))

            self.debug_dataset_path = self._dump_debug_output(df, output_path)

            # Export
            UnifiedLogger.set_context(stage="load")
            self.runtime_options["extended"] = extended
            artifacts = self.export(df, output_path, extended=extended)
            logger.info("pipeline_completed", artifacts=str(artifacts.dataset))

            return artifacts

        except Exception as e:
            logger.error("pipeline_failed", error=str(e), exc_info=True)
            raise
        finally:
            UnifiedLogger.set_context(stage="cleanup")
            self.runtime_options.pop("limit", None)
            self.runtime_options.pop("sample", None)
            self.close()
