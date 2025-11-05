"""Core pipeline orchestration utilities.

This module intentionally focuses on filesystem layout responsibilities so that
pipeline documentation can describe the exact artifact names and retention
rules with executable references.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import pandera.errors

from bioetl.config import PipelineConfig
from bioetl.core.logger import UnifiedLogger
from bioetl.core.output import (
    DeterministicWriteArtifacts,
    build_write_artifacts,
    emit_qc_artifact,
    write_dataset_atomic,
    write_yaml_atomic,
)
from bioetl.qc.report import (
    build_correlation_report as build_default_correlation_report,
    build_quality_report as build_default_quality_report,
    build_qc_metrics_payload,
)
from bioetl.schemas import SchemaRegistryEntry, get_schema


@dataclass(frozen=True)
class WriteArtifacts:
    """Collection of files emitted by the write stage of a pipeline run."""

    dataset: Path
    metadata: Path | None = None
    quality_report: Path | None = None
    correlation_report: Path | None = None
    qc_metrics: Path | None = None


@dataclass(frozen=True)
class RunArtifacts:
    """All artifacts tracked for a completed run."""

    write: WriteArtifacts
    run_directory: Path
    manifest: Path | None
    log_file: Path
    extras: dict[str, Path] = field(default_factory=dict)


@dataclass(frozen=True)
class WriteResult:
    """Materialised artifacts produced by the write stage."""

    dataset: Path
    metadata: Path | None = None
    quality_report: Path | None = None
    correlation_report: Path | None = None
    qc_metrics: Path | None = None
    extras: dict[str, Path] = field(default_factory=dict)


@dataclass(frozen=True)
class RunResult:
    """Final result of a pipeline execution."""

    run_id: str
    write_result: WriteResult
    run_directory: Path
    manifest: Path | None
    log_file: Path
    stage_durations_ms: dict[str, float] = field(default_factory=dict)


class PipelineBase(ABC):
    """Shared orchestration helpers for ETL pipelines."""

    dataset_extension: str = "csv"
    qc_extension: str = "csv"
    manifest_extension: str = "json"
    log_extension: str = "log"
    deterministic_folder_prefix: str = "_"

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        self.config = config
        self.run_id = run_id
        self.pipeline_code = config.pipeline.name
        self.output_root = Path(config.materialization.root)
        self.logs_root = self.output_root.parent / "logs"
        self.retention_runs = 5
        self.pipeline_directory = self._ensure_pipeline_directory()
        self.logs_directory = self._ensure_logs_directory()
        self._stage_durations_ms: dict[str, float] = {}
        self._registered_clients: dict[str, Callable[[], None]] = {}
        self._trace_id, self._root_span_id = self._derive_trace_and_span()
        self._validation_schema: SchemaRegistryEntry | None = None
        self._validation_summary: dict[str, Any] | None = None

    def _ensure_pipeline_directory(self) -> Path:
        """Ensure the deterministic output folder exists for the pipeline."""

        directory = self.output_root / f"{self.deterministic_folder_prefix}{self.pipeline_code}"
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _ensure_logs_directory(self) -> Path:
        """Ensure the log folder exists for the pipeline."""

        directory = self.logs_root / self.pipeline_code
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _derive_trace_and_span(self) -> tuple[str, str]:
        seed = "".join(character for character in self.run_id if character.isalnum()) or "0"
        repeat_count = (32 // len(seed)) + 2
        expanded = (seed * repeat_count)
        trace_id = expanded[:32].ljust(32, "0")
        span_id = expanded[32:48].ljust(16, "0")
        return trace_id, span_id

    def _normalise_run_tag(self, run_tag: str | None = None) -> str:
        """Return a YYYYMMDD tag using overrides or the current date."""

        candidates = []
        if run_tag:
            candidates.append(run_tag)
        if self.config.cli.date_tag:
            candidates.append(self.config.cli.date_tag)
        if not candidates:
            candidates.append(None)
        seen: set[str] = set()
        patterns = ("%Y%m%d", "%Y-%m-%d")
        for candidate in candidates:
            if not candidate:
                continue
            if candidate in seen:
                continue
            seen.add(candidate)
            trimmed = candidate.strip()
            for pattern in patterns:
                try:
                    parsed = datetime.strptime(trimmed, pattern)
                except ValueError:
                    continue
                return parsed.strftime("%Y%m%d")
            digits = "".join(character for character in trimmed if character.isdigit())
            if len(digits) >= 8:
                truncated = digits[:8]
                try:
                    parsed = datetime.strptime(truncated, "%Y%m%d")
                except ValueError:
                    pass
                else:
                    return parsed.strftime("%Y%m%d")
        timezone_name = self.config.determinism.environment.timezone
        try:
            tz = ZoneInfo(timezone_name)
        except Exception:  # pragma: no cover - invalid timezone handled by defaults
            tz = ZoneInfo("UTC")
        return datetime.now(tz).strftime("%Y%m%d")

    def build_run_stem(self, run_tag: str | None, mode: str | None = None) -> str:
        """Return the filename stem for artifacts in a run.

        The stem combines the pipeline code, optional mode (e.g. "all" or
        "incremental"), and a deterministic tag such as a run date.
        """
        tag = self._normalise_run_tag(run_tag)
        parts: Sequence[str]
        if mode:
            parts = (self.pipeline_code, mode, tag)
        else:
            parts = (self.pipeline_code, tag)
        return "_".join(parts)

    def plan_run_artifacts(
        self,
        run_tag: str | None,
        mode: str | None = None,
        include_correlation: bool = False,
        include_qc_metrics: bool = False,
        include_metadata: bool = False,
        include_manifest: bool = False,
        extras: dict[str, Path] | None = None,
    ) -> RunArtifacts:
        """Return the artifact map for a deterministic run.

        The directory layout matches the examples in the pipeline catalog where a
        dataset, QC reports, `meta.yaml`, and manifest live side-by-side inside a
        deterministic folder such as ``data/output/_documents``.
        """

        stem = self.build_run_stem(run_tag=run_tag, mode=mode)
        run_dir = self.pipeline_directory
        dataset = run_dir / f"{stem}.{self.dataset_extension}"
        quality = run_dir / f"{stem}_quality_report.{self.qc_extension}"
        correlation = (
            run_dir / f"{stem}_correlation_report.{self.qc_extension}"
            if include_correlation
            else None
        )
        qc_metrics = run_dir / f"{stem}_qc.{self.qc_extension}" if include_qc_metrics else None
        metadata = run_dir / f"{stem}_meta.yaml" if include_metadata else None
        manifest = (
            run_dir / f"{stem}_run_manifest.{self.manifest_extension}"
            if include_manifest
            else None
        )
        log_file = self.logs_directory / f"{stem}.{self.log_extension}"

        write_artifacts = WriteArtifacts(
            dataset=dataset,
            metadata=metadata,
            quality_report=quality,
            correlation_report=correlation,
            qc_metrics=qc_metrics,
        )
        return RunArtifacts(
            write=write_artifacts,
            run_directory=run_dir,
            manifest=manifest,
            log_file=log_file,
            extras=extras or {},
        )

    def list_run_stems(self) -> Sequence[str]:
        """Return deterministic run stems discovered via dataset files."""

        suffix = f".{self.dataset_extension}"
        dataset_files = sorted(
            (
                path
                for path in self.pipeline_directory.glob(f"{self.pipeline_code}_*{suffix}")
                if path.name.endswith(suffix)
                and not path.stem.endswith("_quality_report")
                and not path.stem.endswith("_correlation_report")
                and not path.stem.endswith("_qc")
            ),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        return [path.stem for path in dataset_files]

    def apply_retention_policy(self) -> None:
        """Prune older runs beyond the configured ``retention_runs`` count."""

        if self.retention_runs <= 0:
            return

        stems = self.list_run_stems()
        for outdated_stem in stems[self.retention_runs :]:
            for candidate in self._artifact_candidates(outdated_stem):
                if candidate.exists():
                    candidate.unlink()
            log_candidate = self.logs_directory / f"{outdated_stem}.{self.log_extension}"
            if log_candidate.exists():
                log_candidate.unlink()

    def _artifact_candidates(self, stem: str) -> Iterable[Path]:
        """Yield the expected artifact paths for ``stem``."""

        yield self.pipeline_directory / f"{stem}.{self.dataset_extension}"
        yield self.pipeline_directory / f"{stem}_quality_report.{self.qc_extension}"
        yield self.pipeline_directory / f"{stem}_correlation_report.{self.qc_extension}"
        yield self.pipeline_directory / f"{stem}_qc.{self.qc_extension}"
        yield self.pipeline_directory / f"{stem}_meta.yaml"
        yield self.pipeline_directory / f"{stem}_run_manifest.{self.manifest_extension}"

    @abstractmethod
    def extract(self, *args: object, **kwargs: object) -> object:
        """Subclasses fetch raw data and return domain-specific payloads."""

    @abstractmethod
    def transform(self, payload: object) -> object:
        """Subclasses transform raw payloads into normalized tabular data."""

    # ------------------------------------------------------------------
    # Optional hooks overridable by subclasses
    # ------------------------------------------------------------------

    def build_quality_report(self, df: pd.DataFrame) -> pd.DataFrame | dict[str, object] | None:
        """Return a QC dataframe for the quality report artefact."""

        business_key = self.config.determinism.hashing.business_key_fields
        return build_default_quality_report(df, business_key_fields=business_key)

    def build_correlation_report(self, df: pd.DataFrame) -> pd.DataFrame | dict[str, object] | None:
        """Return a correlation report artefact payload."""

        return build_default_correlation_report(df)

    def build_qc_metrics(self, df: pd.DataFrame) -> pd.DataFrame | dict[str, object] | None:
        """Return aggregated QC metrics for persistence."""

        business_key = self.config.determinism.hashing.business_key_fields
        return build_qc_metrics_payload(df, business_key_fields=business_key)

    def augment_metadata(
        self,
        metadata: Mapping[str, object],
        df: pd.DataFrame,
    ) -> Mapping[str, object]:
        """Hook allowing subclasses to enrich ``meta.yaml`` content."""

        return metadata

    def close_resources(self) -> None:
        """Release additional resources during cleanup."""

        return None

    def register_client(
        self,
        name: str,
        client: Callable[[], None] | object,
        *,
        close_method: str = "close",
    ) -> None:
        """Register a client for automatic cleanup during ``run()`` finalisation."""

        if name in self._registered_clients:
            msg = f"Client '{name}' is already registered"
            raise ValueError(msg)

        closer: Callable[[], None]
        if callable(client):
            closer = client  # type: ignore[assignment]
        else:
            candidate = getattr(client, close_method, None)
            if candidate is None or not callable(candidate):
                msg = (
                    f"Client '{name}' does not expose callable '{close_method}' and is not itself callable"
                )
                raise TypeError(msg)
            closer = candidate

        self._registered_clients[name] = closer

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _component_for_stage(self, stage: str) -> str:
        return f"{self.pipeline_code}.{stage}"

    def _safe_len(self, payload: object) -> int | None:
        if isinstance(payload, pd.DataFrame):
            return int(payload.shape[0])
        if hasattr(payload, "__len__"):
            try:
                return int(len(payload))  # type: ignore[arg-type]
            except TypeError:
                return None
        return None

    def _cleanup_registered_clients(self) -> None:
        if not self._registered_clients:
            return
        log = UnifiedLogger.get(__name__)
        for name, closer in list(self._registered_clients.items())[::-1]:
            try:
                closer()
            except Exception as exc:  # pragma: no cover - defensive cleanup path
                log.warning("client_cleanup_failed", client=name, error=str(exc))
            finally:
                self._registered_clients.pop(name, None)

    def write(self, payload: object, artifacts: RunArtifacts) -> WriteResult:
        """Default deterministic write implementation used by pipelines."""

        if not isinstance(payload, pd.DataFrame):
            msg = "PipelineBase.write expects a pandas DataFrame payload"
            raise TypeError(msg)

        log = UnifiedLogger.get(__name__)
        prepared: DeterministicWriteArtifacts = build_write_artifacts(
            payload,
            config=self.config,
            run_id=self.run_id,
            pipeline_code=self.pipeline_code,
            dataset_path=artifacts.write.dataset,
            stage_durations_ms=self._stage_durations_ms,
        )

        metadata_payload = self.augment_metadata(prepared.metadata, prepared.dataframe)
        if not isinstance(metadata_payload, Mapping):
            msg = "augment_metadata must return a mapping"
            raise TypeError(msg)
        metadata = dict(metadata_payload)

        metrics_payload = self.build_qc_metrics(prepared.dataframe)
        metrics_summary: dict[str, Any] | None = None
        if isinstance(metrics_payload, Mapping):
            metrics_summary = {key: metrics_payload[key] for key in metrics_payload}
        elif isinstance(metrics_payload, pd.DataFrame):
            metrics_summary = {
                str(row.get("metric", index)): row.get("value")
                for index, row in metrics_payload.to_dict(orient="index").items()
            }

        if self._validation_summary:
            metadata.setdefault("validation", {}).update(self._validation_summary)

        if metrics_summary:
            metadata.setdefault("quality", {}).setdefault("metrics", {}).update(metrics_summary)

        log.debug(
            "write_artifacts_prepared",
            rows=len(prepared.dataframe),
            dataset=str(artifacts.write.dataset),
        )

        write_dataset_atomic(prepared.dataframe, artifacts.write.dataset, config=self.config)
        log.debug("dataset_written", path=str(artifacts.write.dataset))
        metadata_path: Path | None = None
        if artifacts.write.metadata is not None:
            write_yaml_atomic(metadata, artifacts.write.metadata)
            log.debug("metadata_written", path=str(artifacts.write.metadata))
            metadata_path = artifacts.write.metadata

        quality_payload = self.build_quality_report(prepared.dataframe)
        quality_path = emit_qc_artifact(
            quality_payload,
            artifacts.write.quality_report,
            config=self.config,
            log=log,
            artifact_name="quality_report",
        )

        correlation_payload = self.build_correlation_report(prepared.dataframe)
        correlation_path = emit_qc_artifact(
            correlation_payload,
            artifacts.write.correlation_report,
            config=self.config,
            log=log,
            artifact_name="correlation_report",
        )

        metrics_path = emit_qc_artifact(
            metrics_payload,
            artifacts.write.qc_metrics,
            config=self.config,
            log=log,
            artifact_name="qc_metrics",
        )

        return WriteResult(
            dataset=artifacts.write.dataset,
            metadata=metadata_path,
            quality_report=quality_path,
            correlation_report=correlation_path,
            qc_metrics=metrics_path,
            extras=dict(artifacts.extras),
        )

    def run(
        self,
        *args: object,
        mode: str | None = None,
        include_correlation: bool | None = None,
        include_qc_metrics: bool | None = None,
        extras: dict[str, Path] | None = None,
        **kwargs: object,
    ) -> RunResult:
        """Execute the pipeline lifecycle and return collected artifacts."""

        log = UnifiedLogger.get(__name__)
        UnifiedLogger.bind(
            run_id=self.run_id,
            pipeline=self.pipeline_code,
            dataset=self.pipeline_code,
            component=f"{self.pipeline_code}.pipeline",
            trace_id=self._trace_id,
            span_id=self._root_span_id,
        )

        stage_durations_ms: dict[str, float] = {}
        self._stage_durations_ms = stage_durations_ms

        artifacts: RunArtifacts | None = None

        configured_mode = mode
        if self.config.cli.extended:
            configured_mode = "extended"

        UnifiedLogger.bind(stage="bootstrap")
        log.info("pipeline_started", mode=configured_mode)

        try:
            is_extended = configured_mode == "extended"
            include_correlation = (
                include_correlation
                if include_correlation is not None
                else (is_extended or self.config.postprocess.correlation.enabled)
            )
            include_qc_metrics = (
                include_qc_metrics if include_qc_metrics is not None else is_extended
            )
            include_metadata = is_extended
            include_manifest = is_extended
            run_tag = self._normalise_run_tag(None)

            with UnifiedLogger.stage("extract", component=self._component_for_stage("extract")):
                log.info("extract_started")
                extract_start = time.perf_counter()
                extracted = self.extract(*args, **kwargs)
                duration = (time.perf_counter() - extract_start) * 1000.0
                stage_durations_ms["extract"] = duration
                rows = self._safe_len(extracted)
                log.info("extract_completed", duration_ms=duration, rows=rows)

            with UnifiedLogger.stage("transform", component=self._component_for_stage("transform")):
                log.info("transform_started")
                transform_start = time.perf_counter()
                transformed = self.transform(extracted)
                duration = (time.perf_counter() - transform_start) * 1000.0
                stage_durations_ms["transform"] = duration
                rows = self._safe_len(transformed)
                log.info("transform_completed", duration_ms=duration, rows=rows)

            prepared_for_validation = transformed
            if isinstance(transformed, pd.DataFrame):
                prepared_for_validation = self._apply_cli_sample(transformed)

            with UnifiedLogger.stage("validate", component=self._component_for_stage("validate")):
                log.info("validate_started")
                validate_start = time.perf_counter()
                validated = self.validate(prepared_for_validation)
                duration = (time.perf_counter() - validate_start) * 1000.0
                stage_durations_ms["validate"] = duration
                rows = self._safe_len(validated)
                log.info("validate_completed", duration_ms=duration, rows=rows)

            with UnifiedLogger.stage("write", component=self._component_for_stage("write")):
                artifacts = self.plan_run_artifacts(
                    run_tag=run_tag,
                    mode=configured_mode,
                    include_correlation=include_correlation,
                    include_qc_metrics=include_qc_metrics,
                    include_metadata=include_metadata,
                    include_manifest=include_manifest,
                    extras=extras,
                )
                log.info(
                    "write_started",
                    dataset=str(artifacts.write.dataset),
                    metadata=(str(artifacts.write.metadata) if artifacts.write.metadata else None),
                )
                write_start = time.perf_counter()
                write_result = self.write(validated, artifacts)
                duration = (time.perf_counter() - write_start) * 1000.0
                stage_durations_ms["write"] = duration
                log.info(
                    "write_completed",
                    duration_ms=duration,
                    dataset=str(write_result.dataset),
                )

            self.apply_retention_policy()
            log.info("pipeline_completed", stage_durations_ms=stage_durations_ms)

            if artifacts is None:
                raise RuntimeError("Artifacts must be initialised during the write stage")

            return RunResult(
                run_id=self.run_id,
                write_result=write_result,
                run_directory=artifacts.run_directory,
                manifest=artifacts.manifest,
                log_file=artifacts.log_file,
                stage_durations_ms=stage_durations_ms,
            )

        except Exception as exc:
            log.error("pipeline_failed", error=str(exc), exc_info=True)
            raise

        finally:
            with UnifiedLogger.stage("cleanup", component=self._component_for_stage("cleanup")):
                log.info("cleanup_started")
                self._cleanup_registered_clients()
                try:
                    self.close_resources()
                except Exception as cleanup_error:  # pragma: no cover - defensive cleanup path
                    log.warning("cleanup_failed", error=str(cleanup_error))
                log.info("cleanup_completed")

    def validate(self, payload: object) -> pd.DataFrame:
        """Validate ``payload`` against the configured Pandera schema."""

        if not isinstance(payload, pd.DataFrame):
            msg = "PipelineBase.validate expects a pandas DataFrame payload"
            raise TypeError(msg)

        schema_identifier = self.config.validation.schema_out
        log = UnifiedLogger.get(__name__)
        if not schema_identifier:
            log.debug("validation_skipped", reason="no_schema_configured")
            self._validation_schema = None
            self._validation_summary = None
            return payload

        schema_entry = get_schema(schema_identifier)
        schema_obj = schema_entry.schema
        original_strict: bool | None = None
        original_coerce: bool | None = None
        if hasattr(schema_obj, "replace"):
            schema = schema_obj.replace(
                strict=self.config.validation.strict,
                coerce=self.config.validation.coerce,
            )
        else:
            original_strict = getattr(schema_obj, "strict", None)
            original_coerce = getattr(schema_obj, "coerce", None)
            schema_obj.strict = self.config.validation.strict  # type: ignore[attr-defined]
            schema_obj.coerce = self.config.validation.coerce  # type: ignore[attr-defined]
            schema = schema_obj
        log.debug(
            "validation_schema_loaded",
            schema=schema_entry.identifier,
            version=schema_entry.version,
        )

        fail_open = (not getattr(self.config.cli, "fail_on_schema_drift", True)) or (
            not getattr(self.config.cli, "validate_columns", True)
        )

        schema_valid = True
        failure_count: int | None = None
        error_summary: str | None = None

        try:
            validated = schema.validate(payload, lazy=True)
            validated = self._reorder_columns(validated, schema_entry.column_order)
        except pandera.errors.SchemaErrors as exc:
            if not fail_open:
                # Restore schema state before re-raising
                if not hasattr(schema_obj, "replace"):
                    if original_strict is not None:
                        schema_obj.strict = original_strict  # type: ignore[attr-defined]
                    if original_coerce is not None:
                        schema_obj.coerce = original_coerce  # type: ignore[attr-defined]
                raise
            failure_count = len(exc.failure_cases) if hasattr(exc, "failure_cases") else None
            error_summary = str(exc)
            log.warning(
                "schema_validation_failed",
                schema=schema_entry.identifier,
                version=schema_entry.version,
                error=error_summary,
                failure_count=failure_count,
            )
            validated = payload
            schema_valid = False
        finally:
            if not hasattr(schema_obj, "replace"):
                if original_strict is not None:
                    schema_obj.strict = original_strict  # type: ignore[attr-defined]
                if original_coerce is not None:
                    schema_obj.coerce = original_coerce  # type: ignore[attr-defined]

        self._validation_schema = schema_entry
        self._validation_summary = {
            "schema_identifier": schema_entry.identifier,
            "schema_name": schema_entry.name,
            "schema_version": schema_entry.version,
            "column_order": list(schema_entry.column_order),
            "strict": bool(self.config.validation.strict),
            "coerce": bool(self.config.validation.coerce),
            "row_count": int(len(validated)) if isinstance(validated, pd.DataFrame) else None,
            "schema_valid": schema_valid,
        }
        if error_summary is not None:
            self._validation_summary["error"] = error_summary
        if failure_count is not None:
            self._validation_summary["failure_count"] = failure_count

        if schema_valid:
            log.debug(
                "validation_completed",
                schema=schema_entry.identifier,
                version=schema_entry.version,
                rows=len(validated),
            )

        return validated

    def _reorder_columns(self, df: pd.DataFrame, column_order: Sequence[str]) -> pd.DataFrame:
        if not column_order:
            return df
        ordered = list(column_order)
        validate_columns = getattr(self.config.cli, "validate_columns", True)
        if validate_columns:
            missing = [column for column in ordered if column not in df.columns]
            if missing:
                msg = f"Dataframe missing columns required by schema: {missing}"
                raise ValueError(msg)
            extras = [column for column in df.columns if column not in ordered]
            if not extras:
                return df[ordered]
            return df[[*ordered, *extras]]

        existing = [column for column in ordered if column in df.columns]
        extras = [column for column in df.columns if column not in existing]
        if not existing:
            return df
        return df[[*existing, *extras]]

    def _deterministic_sample_seed(self) -> int:
        material = f"{self.config.pipeline.name}:{self.config.pipeline.version}"
        digest = hashlib.sha256(material.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big", signed=False) % (2**32 - 1)

    def _apply_cli_sample(self, df: pd.DataFrame) -> pd.DataFrame:
        sample_size = getattr(self.config.cli, "sample", None)
        if not sample_size or df.empty:
            return df

        if sample_size >= len(df):
            log = UnifiedLogger.get(__name__)
            log.debug(
                "sample_size_exceeds_population",
                requested=sample_size,
                population=len(df),
            )
            return df

        seed = self._deterministic_sample_seed()
        sampled = df.sample(n=sample_size, random_state=seed, replace=False).sort_index()
        log = UnifiedLogger.get(__name__)
        log.info(
            "sample_applied",
            sample_size=sample_size,
            population=len(df),
            seed=seed,
        )
        return sampled.reset_index(drop=True)
