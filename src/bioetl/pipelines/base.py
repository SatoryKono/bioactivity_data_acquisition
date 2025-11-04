"""Core pipeline orchestration utilities.

This module provides the abstract base class PipelineBase that defines the
contract and lifecycle for all ETL pipelines in the bioetl framework.
"""

from __future__ import annotations

import shutil
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pandera as pa

from bioetl.config import PipelineConfig
from bioetl.configs.models import CacheConfig
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.client_factory import APIClientFactory
from bioetl.core.logger import bind_global_context, get_logger
from bioetl.core.output import (
    DeterministicWriteArtifacts,
    build_write_artifacts,
    emit_qc_artifact,
    write_dataset_atomic,
    write_yaml_atomic,
)
from bioetl.core.schema_registry import get_registry


@dataclass(frozen=True)
class WriteArtifacts:
    """Artifacts paths for the write stage."""

    dataset: Path
    metadata: Path
    quality_report: Path | None = None
    correlation_report: Path | None = None
    qc_metrics: Path | None = None


@dataclass(frozen=True)
class RunArtifacts:
    """Artifacts paths for a complete pipeline run."""

    write: WriteArtifacts
    run_directory: Path
    manifest: Path
    log_file: Path
    extras: dict[str, Path] = field(default_factory=dict)


@dataclass(frozen=True)
class WriteResult:
    """Materialised artifacts produced by the write stage."""

    dataset: Path
    metadata: Path
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
    manifest: Path
    log_file: Path
    stage_durations_ms: dict[str, float] = field(default_factory=dict)


class PipelineBase(ABC):
    """Abstract base class defining the contract for all ETL pipelines.

    The PipelineBase class standardizes the ETL process by defining a fixed,
    four-stage lifecycle: extract → transform → validate → write, orchestrated
    by the run() method.
    """

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        """Initialize the pipeline with its configuration and a unique run ID.

        Args:
            config: Pipeline configuration validated from YAML.
            run_id: Unique identifier for this pipeline run (UUID format).
        """
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

        # Initialize logger
        self.logger = get_logger(__name__)

        # Determine source identifier from config
        source = self._determine_source()

        # Bind initial logging context
        bind_global_context(
            run_id=self.run_id,
            pipeline=self.config.pipeline.name,
            stage="bootstrap",
            actor=self.config.pipeline.name,
            source=source,
            dataset=self.config.pipeline.name,
            component="pipeline_base",
        )

        # Initialize API client factory
        self._client_factory = APIClientFactory(config)

        # Initialize schema registry
        self._schema_registry = get_registry()

        self.logger.info("pipeline_initialized", pipeline=self.config.pipeline.name, run_id=self.run_id)

    def _ensure_pipeline_directory(self) -> Path:
        """Ensure the deterministic output folder exists for the pipeline."""
        directory = self.output_root / self.pipeline_code
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _ensure_logs_directory(self) -> Path:
        """Ensure the log folder exists for the pipeline."""
        directory = self.logs_root / self.pipeline_code
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _derive_trace_and_span(self) -> tuple[str, str]:
        """Derive trace and span IDs from run_id."""
        seed = "".join(character for character in self.run_id if character.isalnum()) or "0"
        repeat_count = (32 // len(seed)) + 2
        expanded = seed * repeat_count
        trace_id = expanded[:32].ljust(32, "0")
        span_id = expanded[32:48].ljust(16, "0")
        return trace_id, span_id

    def _determine_source(self) -> str:
        """Determine the source identifier from configuration."""
        # Try to get source from first enabled source in config
        for source_name, source_config in self.config.sources.items():
            if source_config.enabled:
                return source_name
        # Fallback to pipeline name if no source found
        return self.config.pipeline.name

    @abstractmethod
    def extract(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """Extract data from the source and return it as a DataFrame.

        This method MUST be implemented by the developer. It is responsible
        for all source interaction, including API calls, database queries,
        or reading from files.

        Returns:
            DataFrame containing raw, unmodified data from the source.
        """
        raise NotImplementedError

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw extracted data into the target schema.

        This method MUST be implemented by the developer. It includes all
        business logic: cleaning, normalization, type casting, column renaming,
        and enrichment.

        Args:
            df: Raw DataFrame from extract stage.

        Returns:
            DataFrame conforming to the structure expected by the output
            Pandera schema.
        """
        raise NotImplementedError

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate the transformed data against the output Pandera schema.

        This method is managed by the framework and SHOULD NOT be overridden.
        It enforces strict data types, column order, and custom checks.

        Args:
            df: Transformed DataFrame to validate.

        Returns:
            Validated DataFrame with enforced column order and types.

        Raises:
            pandera.errors.SchemaError: If validation fails.
        """
        bind_global_context(stage="validate")
        self.logger.info("validation_started", rows=len(df))

        validate_start = time.perf_counter()

        try:
            if not self.config.validation.schema_out:
                self.logger.warning("no_schema_configured", message="No schema_out specified, skipping validation")
                return df

            # Load schema from dotted path
            schema = self._load_schema(self.config.validation.schema_out)

            # Apply validation (strict and coerce are schema-level settings)
            validated_df_raw: pd.DataFrame = schema.validate(df, lazy=False)
            validated_df = pd.DataFrame(validated_df_raw)

            # Ensure column order matches config if specified
            if self.config.determinism.column_order:
                validated_df = validated_df[self.config.determinism.column_order]

            duration_ms = (time.perf_counter() - validate_start) * 1000.0
            self._stage_durations_ms["validate"] = duration_ms
            self.logger.info("validation_completed", rows=len(validated_df), duration_ms=duration_ms)

            return validated_df

        except pa.errors.SchemaError as e:
            duration_ms = (time.perf_counter() - validate_start) * 1000.0
            self.logger.error(
                "validation_failed",
                errors=str(e),
                duration_ms=duration_ms,
                exc_info=True,
            )
            raise

    def _load_schema(self, dotted_path: str) -> Any:
        """Load a Pandera schema from a dotted module path.

        Uses SchemaRegistry for caching and improved error handling.

        Args:
            dotted_path: Dotted path like 'bioetl.schemas.chembl.activity_out.ActivitySchema'

        Returns:
            Pandera DataFrameSchema instance or SchemaModel class.

        Raises:
            ImportError: If module cannot be imported.
            AttributeError: If schema class cannot be found.
            ValueError: If schema path format is invalid.
        """
        try:
            schema = self._schema_registry.load_schema(dotted_path)
            return schema
        except (ImportError, AttributeError, ValueError) as e:
            self.logger.error(
                "schema_load_failed",
                schema_path=dotted_path,
                error=str(e),
                exc_info=True,
            )
            raise

    def plan_run_artifacts(
        self,
        run_tag: str,
        mode: str | None = None,
        include_correlation: bool = False,
        include_qc_metrics: bool = True,
        extras: dict[str, Path] | None = None,
    ) -> RunArtifacts:
        """Plan artifact paths for a pipeline run."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        stem = f"{self.pipeline_code}_{run_tag}_{timestamp}" if mode else f"{self.pipeline_code}_{run_tag}"

        run_directory = self.pipeline_directory / run_tag
        run_directory.mkdir(parents=True, exist_ok=True)

        write_artifacts = WriteArtifacts(
            dataset=run_directory / f"{stem}.csv",
            metadata=run_directory / f"{stem}_meta.yaml",
            quality_report=run_directory / f"{stem}_quality_report.csv",
            correlation_report=run_directory / f"{stem}_correlation_report.csv" if include_correlation else None,
            qc_metrics=run_directory / f"{stem}_qc_metrics.csv" if include_qc_metrics else None,
        )

        log_file = self.logs_directory / f"{stem}.log"
        manifest = run_directory / f"{stem}_manifest.json"

        return RunArtifacts(
            write=write_artifacts,
            run_directory=run_directory,
            manifest=manifest,
            log_file=log_file,
            extras=extras or {},
        )

    def apply_retention_policy(self) -> None:
        """Apply retention policy to old pipeline runs."""
        if not self.pipeline_directory.exists():
            return

        runs = sorted(self.pipeline_directory.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
        if len(runs) > self.retention_runs:
            for old_run in runs[self.retention_runs :]:
                shutil.rmtree(old_run, ignore_errors=True)

    # ------------------------------------------------------------------
    # Optional hooks overridable by subclasses
    # ------------------------------------------------------------------

    def build_quality_report(self, df: pd.DataFrame) -> pd.DataFrame | dict[str, object] | None:
        """Return a QC dataframe for the quality report artefact."""
        return None

    def build_correlation_report(self, df: pd.DataFrame) -> pd.DataFrame | dict[str, object] | None:
        """Return a correlation report artefact payload."""
        return None

    def build_qc_metrics(self, df: pd.DataFrame) -> pd.DataFrame | dict[str, object] | None:
        """Return aggregated QC metrics for persistence."""
        return None

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
            closer = client
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
        """Return component name for a stage."""
        return f"{self.pipeline_code}.{stage}"

    def _safe_len(self, payload: object) -> int | None:
        """Safely get length of payload."""
        if isinstance(payload, pd.DataFrame):
            return int(payload.shape[0])
        if hasattr(payload, "__len__"):
            try:
                return int(len(payload))
            except TypeError:
                return None
        return None

    def _cleanup_registered_clients(self) -> None:
        """Clean up registered clients."""
        if not self._registered_clients:
            return
        log = get_logger(__name__)
        for name, closer in list(self._registered_clients.items())[::-1]:
            try:
                closer()
            except Exception as exc:  # pragma: no cover - defensive cleanup path
                log.warning("client_cleanup_failed", client=name, error=str(exc))
            finally:
                self._registered_clients.pop(name, None)

    def _stage_context(self, stage: str, component: str | None = None) -> Any:
        """Context manager for stage logging."""
        from contextlib import contextmanager

        @contextmanager
        def stage_manager():
            bind_global_context(stage=stage, component=component or self._component_for_stage(stage))
            try:
                yield
            finally:
                pass

        return stage_manager()

    def write(self, payload: object, artifacts: RunArtifacts) -> WriteResult:
        """Default deterministic write implementation used by pipelines."""
        if not isinstance(payload, pd.DataFrame):
            msg = "PipelineBase.write expects a pandas DataFrame payload"
            raise TypeError(msg)

        log = get_logger(__name__)
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

        log.debug(
            "write_artifacts_prepared",
            rows=len(prepared.dataframe),
            dataset=str(artifacts.write.dataset),
        )

        write_dataset_atomic(prepared.dataframe, artifacts.write.dataset, config=self.config)
        log.debug("dataset_written", path=str(artifacts.write.dataset))
        write_yaml_atomic(metadata, artifacts.write.metadata)
        log.debug("metadata_written", path=str(artifacts.write.metadata))

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

        metrics_payload = self.build_qc_metrics(prepared.dataframe)
        metrics_path = emit_qc_artifact(
            metrics_payload,
            artifacts.write.qc_metrics,
            config=self.config,
            log=log,
            artifact_name="qc_metrics",
        )

        return WriteResult(
            dataset=artifacts.write.dataset,
            metadata=artifacts.write.metadata,
            quality_report=quality_path,
            correlation_report=correlation_path,
            qc_metrics=metrics_path,
            extras=dict(artifacts.extras),
        )

    def run(
        self,
        *args: object,
        mode: str | None = None,
        include_correlation: bool = False,
        include_qc_metrics: bool = True,
        extras: dict[str, Path] | None = None,
        **kwargs: object,
    ) -> RunResult:
        """Execute the pipeline lifecycle and return collected artifacts."""
        log = get_logger(__name__)
        bind_global_context(
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

        bind_global_context(stage="bootstrap")
        log.info("pipeline_started", mode=mode)

        try:
            with self._stage_context("extract", self._component_for_stage("extract")):
                log.info("extract_started")
                extract_start = time.perf_counter()
                extracted = self.extract(*args, **kwargs)
                duration = (time.perf_counter() - extract_start) * 1000.0
                stage_durations_ms["extract"] = duration
                rows = self._safe_len(extracted)
                log.info("extract_completed", duration_ms=duration, rows=rows)

            with self._stage_context("transform", self._component_for_stage("transform")):
                log.info("transform_started")
                transform_start = time.perf_counter()
                transformed = self.transform(extracted)
                duration = (time.perf_counter() - transform_start) * 1000.0
                stage_durations_ms["transform"] = duration
                rows = self._safe_len(transformed)
                log.info("transform_completed", duration_ms=duration, rows=rows)

            with self._stage_context("validate", self._component_for_stage("validate")):
                log.info("validation_started")
                validate_start = time.perf_counter()
                validated = self.validate(transformed)
                duration = (time.perf_counter() - validate_start) * 1000.0
                stage_durations_ms["validate"] = duration
                rows = self._safe_len(validated)
                log.info("validation_completed", duration_ms=duration, rows=rows)

            with self._stage_context("write", self._component_for_stage("write")):
                artifacts = self.plan_run_artifacts(
                    run_tag=self.run_id,
                    mode=mode,
                    include_correlation=include_correlation,
                    include_qc_metrics=include_qc_metrics,
                    extras=extras,
                )
                log.info(
                    "write_started",
                    dataset=str(artifacts.write.dataset),
                    metadata=str(artifacts.write.metadata),
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
            with self._stage_context("cleanup", self._component_for_stage("cleanup")):
                log.info("cleanup_started")
                self._cleanup_registered_clients()
                try:
                    self.close_resources()
                except Exception as cleanup_error:  # pragma: no cover - defensive cleanup path
                    log.warning("cleanup_failed", error=str(cleanup_error))
                log.info("cleanup_completed")

    def init_http_client(
        self,
        source_name: str,
        base_url: str,
        cache_config: CacheConfig | None = None,
    ) -> UnifiedAPIClient:
        """Initialize HTTP client for a source.

        Args:
            source_name: Name of the source (must exist in config.sources).
            base_url: Base URL for the API.
            cache_config: Optional cache configuration (uses config.cache if not provided).

        Returns:
            UnifiedAPIClient instance registered for cleanup.
        """
        client = self._client_factory.create_client(
            source_name=source_name,
            base_url=base_url,
            cache_config=cache_config,
        )

        # Register for automatic cleanup
        self.register_client(source_name, client)

        self.logger.info(
            "http_client_initialized",
            source=source_name,
            base_url=base_url,
        )

        return client


