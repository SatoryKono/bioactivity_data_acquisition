"""Core pipeline orchestration utilities.

This module intentionally focuses on filesystem layout responsibilities so that
pipeline documentation can describe the exact artifact names and retention
rules with executable references.
"""

from __future__ import annotations

import hashlib
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping, Sequence, Sized
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo

import pandas as pd
import pandera.errors
from pandera import DataFrameSchema

from bioetl.config import PipelineConfig
from bioetl.core import APIClientFactory
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.load_meta_store import LoadMetaStore
from bioetl.core.logger import UnifiedLogger
from bioetl.core.output import (
    DeterministicWriteArtifacts,
    build_write_artifacts,
    emit_qc_artifact,
    ensure_hash_columns,
    write_dataset_atomic,
    write_yaml_atomic,
)
from bioetl.pipelines.common.validation import format_failure_cases, summarize_schema_errors
from bioetl.qc.report import build_correlation_report as build_default_correlation_report
from bioetl.qc.report import build_qc_metrics_payload
from bioetl.qc.report import build_quality_report as build_default_quality_report
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
    """Materialised artifacts produced by the write stage.

    According to documentation, this should contain:
    - dataset: Path
    - quality_report: Path
    - metadata: Path

    Additional fields are kept for backward compatibility.
    """

    dataset: Path
    quality_report: Path | None = None
    metadata: Path | None = None
    correlation_report: Path | None = None
    qc_metrics: Path | None = None
    extras: dict[str, Path] = field(default_factory=dict)


@dataclass(frozen=True)
class RunResult:
    """Final result of a pipeline execution.

    According to documentation, this should contain:
    - write_result: WriteResult
    - run_directory: Path
    - manifest: Path
    - additional_datasets: Dict[str, Path]
    - qc_summary: Optional[Path]
    - debug_dataset: Optional[Path]
    """

    write_result: WriteResult
    run_directory: Path
    manifest: Path | None = None
    additional_datasets: dict[str, Path] = field(default_factory=dict)
    qc_summary: Path | None = None
    debug_dataset: Path | None = None
    # Additional fields kept for backward compatibility
    run_id: str | None = None
    log_file: Path | None = None
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
        self._extract_metadata: dict[str, Any] = {}
        load_meta_root = self.output_root.parent / "load_meta" / self.pipeline_code
        self.load_meta_store = LoadMetaStore(load_meta_root, dataset_format="parquet")

    def _ensure_pipeline_directory(self) -> Path:
        """Return the deterministic output folder path for the pipeline.

        Does not create the directory. Use _ensure_pipeline_directory_exists()
        to create it when needed.
        """
        return self.output_root / f"{self.deterministic_folder_prefix}{self.pipeline_code}"

    def _ensure_pipeline_directory_exists(self) -> Path:
        """Ensure the deterministic output folder exists for the pipeline."""
        directory = self.pipeline_directory
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
        expanded = seed * repeat_count
        trace_id = expanded[:32].ljust(32, "0")
        span_id = expanded[32:48].ljust(16, "0")
        return trace_id, span_id

    def _normalise_run_tag(self, run_tag: str | None = None) -> str:
        """Return a YYYYMMDD tag using overrides or the current date."""

        candidates: list[str | None] = []
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
        run_directory: Path | None = None,
    ) -> RunArtifacts:
        """Return the artifact map for a deterministic run.

        The directory layout matches the examples in the pipeline catalog where a
        dataset, QC reports, `meta.yaml`, and manifest live side-by-side inside a
        deterministic folder such as ``data/output/_documents``.
        """

        stem = self.build_run_stem(run_tag=run_tag, mode=mode)
        run_dir = run_directory if run_directory is not None else self.pipeline_directory
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
            run_dir / f"{stem}_run_manifest.{self.manifest_extension}" if include_manifest else None
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

        if not self.pipeline_directory.exists():
            return []

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
    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:
        """Subclasses fetch raw data and return domain-specific payloads.

        Subclasses should check for input_file in config.cli.input_file and
        call extract_by_ids() if present, otherwise call extract_all().
        """
        # According to documentation, extract should return pd.DataFrame

    @abstractmethod
    def extract_all(self) -> pd.DataFrame:
        """Extract all records from the source using pagination.

        This method should paginate through all available records
        from the external source without requiring a list of IDs.
        """

    @abstractmethod
    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:
        """Extract records by a specific list of IDs using batch extraction.

        Parameters
        ----------
        ids:
            Sequence of identifiers to extract (e.g., assay_chembl_id, activity_id, molecule_chembl_id).

        Returns
        -------
        pd.DataFrame:
            DataFrame containing extracted records.
        """

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Subclasses transform raw payloads into normalized tabular data."""
        # According to documentation, transform should accept df: pd.DataFrame and return pd.DataFrame

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
        payload = build_qc_metrics_payload(df, business_key_fields=business_key)
        # Convert Mapping[str, Any] to dict[str, object]
        return dict(payload)

    def augment_metadata(
        self,
        metadata: Mapping[str, object],
        df: pd.DataFrame,
    ) -> Mapping[str, object]:
        """Hook allowing subclasses to enrich ``meta.yaml`` content."""
        enriched = dict(metadata)
        if self._extract_metadata:
            for key, value in self._extract_metadata.items():
                if (
                    key == "filters"
                    and key in enriched
                    and isinstance(enriched[key], Mapping)
                    and isinstance(value, Mapping)
                ):
                    merged_filters = dict(cast(Mapping[str, Any], enriched[key]))
                    merged_filters.update(cast(Mapping[str, Any], value))
                    enriched[key] = merged_filters
                else:
                    enriched[key] = value
        return enriched

    def record_extract_metadata(
        self,
        *,
        chembl_release: str | None = None,
        filters: Mapping[str, Any] | Sequence[tuple[str, Any]] | None = None,
        requested_at_utc: datetime | str | None = None,
        **extra: Any,
    ) -> None:
        """Record metadata produced during ``extract`` for inclusion in ``meta.yaml``."""

        def _normalise_timestamp(value: datetime | str) -> str:
            if isinstance(value, datetime):
                if value.tzinfo is None:
                    value = value.replace(tzinfo=timezone.utc)
                else:
                    value = value.astimezone(timezone.utc)
                return value.isoformat().replace("+00:00", "Z")
            return value

        def _normalise_value(candidate: Any) -> Any:
            if isinstance(candidate, Mapping):
                normalised_items: list[tuple[str, Any]] = []
                mapping_candidate = cast(Mapping[Any, Any], candidate)
                for mapping_key_any, mapping_value_any in mapping_candidate.items():
                    key_as_str = str(mapping_key_any)
                    normalised_value = _normalise_value(mapping_value_any)
                    normalised_items.append((key_as_str, normalised_value))
                normalised_items.sort(key=lambda item: item[0])
                return dict(normalised_items)
            if isinstance(candidate, Sequence) and not isinstance(candidate, (str, bytes)):
                normalised_sequence: list[Any] = []
                sequence_candidate: Sequence[Any] = cast(Sequence[Any], candidate)
                for sequence_item_any in sequence_candidate:
                    normalised_sequence.append(_normalise_value(sequence_item_any))
                return normalised_sequence
            if isinstance(candidate, datetime):
                return _normalise_timestamp(candidate)
            return candidate

        metadata = dict(self._extract_metadata)
        if chembl_release is not None:
            metadata["chembl_release"] = chembl_release
        if filters is not None:
            filters_mapping = dict(filters) if not isinstance(filters, Mapping) else filters
            metadata["filters"] = _normalise_value(filters_mapping)
        if requested_at_utc is not None:
            metadata["requested_at_utc"] = _normalise_timestamp(requested_at_utc)
        for key, value in extra.items():
            if value is None:
                continue
            metadata[key] = _normalise_value(value)
        self._extract_metadata = metadata

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
            # Wrap to ensure return type is None
            def wrapped() -> None:
                client()
                return None

            closer = wrapped
        else:
            candidate = getattr(client, close_method, None)
            if candidate is None or not callable(candidate):
                msg = f"Client '{name}' does not expose callable '{close_method}' and is not itself callable"
                raise TypeError(msg)

            # Wrap to ensure return type is None
            def wrapped() -> None:
                candidate()
                return None

            closer = wrapped

        self._registered_clients[name] = closer

    # ------------------------------------------------------------------
    # Stage utilities (required by documentation)
    # ------------------------------------------------------------------

    def read_input_table(
        self,
        path: Path,
        *,
        limit: int | None = None,
        sample: int | None = None,
    ) -> pd.DataFrame:
        """Read input table from file with logging and deterministic limits.

        This method handles file inputs with consistent logging, deterministic
        limiting, and empty-file handling. It respects CLI `limit`/`sample`
        options for deterministic sampling.

        Parameters
        ----------
        path:
            Path to the input file (CSV or Parquet).
        limit:
            Optional limit on number of rows to read.
        sample:
            Optional sample size for deterministic sampling.

        Returns
        -------
        pd.DataFrame:
            The loaded DataFrame, optionally limited or sampled.
        """
        log = UnifiedLogger.get(__name__)
        resolved_path = path.resolve()

        if not resolved_path.exists():
            log.warning("input_file_not_found", path=str(resolved_path))
            return pd.DataFrame()

        log.info("reading_input", path=str(resolved_path), limit=limit, sample=sample)

        # Determine file format from extension
        # Note: pandas read methods have complex overloads; type checker cannot fully infer return type
        df = (
            pd.read_parquet(resolved_path)  # pyright: ignore[reportUnknownMemberType]
            if resolved_path.suffix.lower() == ".parquet"
            else pd.read_csv(resolved_path, low_memory=False)  # pyright: ignore[reportUnknownMemberType]
        )
        if df.empty:
            log.debug("input_file_empty", path=str(resolved_path))
            return df

        # Apply limit if specified
        if limit is not None and limit > 0:
            if limit < len(df):
                df = df.head(limit)
                log.info("input_limit_active", limit=limit, rows=len(df))

        # Apply sample if specified
        if sample is not None and sample > 0 and sample < len(df):
            seed = self._deterministic_sample_seed()
            df = df.sample(n=sample, random_state=seed, replace=False).sort_index()
            log.info(
                "sample_applied",
                sample_size=sample,
                population=len(df),
                seed=seed,
            )

        return df.reset_index(drop=True)

    def _read_input_ids(
        self,
        *,
        id_column_name: str,
        limit: int | None = None,
        sample: int | None = None,
    ) -> list[str]:
        """Read IDs from input file for batch extraction.

        This method reads the input file specified in config.cli.input_file
        and extracts the specified ID column as a sorted list of unique IDs.

        Parameters
        ----------
        id_column_name:
            Name of the column containing IDs (e.g., 'assay_chembl_id', 'activity_id', 'molecule_chembl_id').
        limit:
            Optional limit on number of IDs to read.
        sample:
            Optional sample size for deterministic sampling.

        Returns
        -------
        list[str]:
            Sorted list of unique IDs from the input file.
        """
        log = UnifiedLogger.get(__name__)

        if not self.config.cli.input_file:
            log.debug("no_input_file", id_column=id_column_name)
            return []

        input_path = Path(self.config.cli.input_file)
        if not input_path.is_absolute():
            # Resolve relative to input_root if configured
            input_root = Path(self.config.paths.input_root)
            # Check if path already starts with input_root to avoid duplication
            input_file_str = str(input_path).replace("\\", "/")
            input_root_str = str(input_root).replace("\\", "/")
            # Normalize: ensure both use forward slashes for comparison
            if input_file_str.startswith(input_root_str + "/") or input_file_str == input_root_str:
                # Path already contains input_root, use as-is (resolve to absolute)
                input_path = input_path.resolve()
            else:
                # Path is relative, resolve via input_root
                input_path = (input_root / input_path).resolve()

        df = self.read_input_table(input_path, limit=limit, sample=sample)

        if df.empty:
            log.warning("input_file_empty_ids", path=str(input_path), id_column=id_column_name)
            return []

        if id_column_name not in df.columns:
            available_columns = list(df.columns)
            log.error(
                "input_file_missing_id_column",
                path=str(input_path),
                id_column=id_column_name,
                available_columns=available_columns,
            )
            msg = f"Input file {input_path} missing required column '{id_column_name}'. Available columns: {available_columns}"
            raise ValueError(msg)

        # Extract unique IDs, drop NaN, convert to string, sort for determinism
        ids: list[str] = df[id_column_name].dropna().astype(str).unique().tolist()
        ids.sort()  # Deterministic ordering

        log.info(
            "input_ids_read",
            path=str(input_path),
            id_column=id_column_name,
            count=len(ids),
            limit=limit,
            sample=sample,
        )

        return ids

    def _get_id_column_name(self) -> str:
        """Return the ID column name based on pipeline type.

        This is a helper method that maps pipeline names to their
        corresponding ID column names. Subclasses can override this
        if they need custom logic.

        Returns
        -------
        str:
            Name of the ID column (e.g., 'assay_chembl_id', 'activity_id', 'molecule_chembl_id').
        """
        pipeline_name = self.pipeline_code.lower()

        # Map pipeline names to ID column names
        id_column_map: dict[str, str] = {
            "assay_chembl": "assay_chembl_id",
            "activity_chembl": "activity_id",
            "testitem_chembl": "molecule_chembl_id",
            "target_chembl": "target_chembl_id",
            "document_chembl": "document_chembl_id",
        }

        # Try exact match first
        if pipeline_name in id_column_map:
            return id_column_map[pipeline_name]

        # Try partial match (e.g., "assay" -> "assay_chembl_id")
        for key, value in id_column_map.items():
            if key.startswith(pipeline_name) or pipeline_name.startswith(key.split("_")[0]):
                return value

        # Fallback: construct from pipeline name
        # Convert "assay_chembl" -> "assay_chembl_id"
        if "_" in pipeline_name:
            parts = pipeline_name.split("_")
            return "_".join(parts[:-1]) + "_id" if len(parts) > 1 else f"{pipeline_name}_id"

        return f"{pipeline_name}_id"

    def execute_enrichment_stages(
        self,
        df: pd.DataFrame,
        *,
        stages: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Execute registered enrichment hooks.

        This method runs registered enrichment stages in sequence. Subclasses
        can override this to add custom enrichment logic.

        Parameters
        ----------
        df:
            The DataFrame to enrich.
        stages:
            Optional sequence of stage names to execute. If None, all
            registered stages are executed.

        Returns
        -------
        pd.DataFrame:
            The enriched DataFrame.
        """
        log = UnifiedLogger.get(__name__)
        if df.empty:
            return df

        # Default implementation: no enrichment stages
        # Subclasses can override to add custom enrichment logic
        log.debug("enrichment_stages_completed", stages=stages or [])
        return df

    def run_schema_validation(
        self,
        df: pd.DataFrame,
        schema_identifier: str,
        *,
        dataset_name: str = "secondary",
    ) -> pd.DataFrame:
        """Validate a secondary dataset against a schema.

        This method validates a DataFrame against a Pandera schema for
        secondary datasets (not the primary output schema).

        Parameters
        ----------
        df:
            The DataFrame to validate.
        schema_identifier:
            The schema identifier from the registry.
        dataset_name:
            Optional name for the dataset (for logging).

        Returns
        -------
        pd.DataFrame:
            The validated DataFrame (may be unchanged if validation fails in fail-open mode).
        """
        log = UnifiedLogger.get(__name__)
        if df.empty:
            return df

        try:
            schema_entry = get_schema(schema_identifier)
            schema = schema_entry.schema
            if hasattr(schema, "replace") and callable(getattr(schema, "replace", None)):
                schema = cast(Any, schema).replace(
                    strict=self.config.validation.strict,
                    coerce=self.config.validation.coerce,
                )

            validated = schema.validate(df, lazy=True)
            if not isinstance(validated, pd.DataFrame):
                msg = "Schema validation did not return a DataFrame"
                raise TypeError(msg)
            validated = self._reorder_columns(validated, schema_entry.column_order)
            log.debug(
                "schema_validation_completed",
                dataset=dataset_name,
                schema=schema_entry.identifier,
                version=schema_entry.version,
                rows=len(validated),
            )
            return validated
        except pandera.errors.SchemaErrors as exc:
            fail_open = (not getattr(self.config.cli, "fail_on_schema_drift", True)) or (
                not getattr(self.config.cli, "validate_columns", True)
            )
            if not fail_open:
                raise
            summary = summarize_schema_errors(exc)
            failure_cases_df = getattr(exc, "failure_cases", None)
            failure_details: dict[str, Any] | None = None
            if isinstance(failure_cases_df, pd.DataFrame) and not failure_cases_df.empty:
                failure_details = format_failure_cases(failure_cases_df)
                summary["failure_count"] = int(len(failure_cases_df))

            log_payload: dict[str, Any] = {
                "dataset": dataset_name,
                "schema": schema_identifier,
                **summary,
            }
            if failure_details:
                log_payload["failure_details"] = failure_details

            log.warning("schema_validation_failed", **log_payload)
            return df

    def finalize_with_standard_metadata(
        self,
        metadata: Mapping[str, Any],
        *,
        df: pd.DataFrame | None = None,
    ) -> dict[str, Any]:
        """Finalize metadata with standard fields.

        This method adds standard metadata fields to the provided metadata
        dictionary, ensuring consistency across all pipeline runs.

        Parameters
        ----------
        metadata:
            The base metadata dictionary.
        df:
            Optional DataFrame to extract metadata from.

        Returns
        -------
        dict[str, Any]:
            The finalized metadata dictionary.
        """
        finalized: dict[str, Any] = dict(metadata)

        # Add standard fields
        finalized["pipeline_version"] = self.config.pipeline.version
        finalized["pipeline_name"] = self.config.pipeline.name
        finalized["run_id"] = self.run_id

        if df is not None:
            finalized["row_count"] = len(df)
            finalized["schema_version"] = (
                self._validation_schema.version if self._validation_schema else None
            )

        return finalized

    def set_export_metadata_from_dataframe(
        self,
        df: pd.DataFrame,
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Set export metadata from DataFrame.

        This method extracts metadata from a DataFrame and merges it with
        optional existing metadata.

        Parameters
        ----------
        df:
            The DataFrame to extract metadata from.
        metadata:
            Optional existing metadata dictionary.

        Returns
        -------
        dict[str, Any]:
            The merged metadata dictionary.
        """
        base_metadata: dict[str, Any] = dict(metadata) if metadata else {}

        # Extract basic statistics
        base_metadata["row_count"] = len(df)
        base_metadata["column_count"] = len(df.columns)
        base_metadata["columns"] = list(df.columns)

        if self._validation_schema:
            base_metadata["schema_identifier"] = self._validation_schema.identifier
            base_metadata["schema_version"] = self._validation_schema.version

        return base_metadata

    def record_validation_issue(
        self,
        *,
        severity: str = "warning",
        dataset: str = "primary",
        column: str | None = None,
        check: str | None = None,
        count: int | None = None,
        error: str | None = None,
    ) -> None:
        """Record a validation issue for QC reporting.

        This method accumulates validation issues that are logged during
        validation but do not cause pipeline failure.

        Parameters
        ----------
        severity:
            Severity level (e.g., "warning", "error", "info").
        dataset:
            Name of the dataset (e.g., "primary", "secondary").
        column:
            Optional column name if issue is column-specific.
        check:
            Optional check name that failed.
        count:
            Optional count of affected rows.
        error:
            Optional error message.
        """
        log = UnifiedLogger.get(__name__)
        log.warning(
            "validation_issue_recorded",
            severity=severity,
            dataset=dataset,
            column=column,
            check=check,
            count=count,
            error=error,
        )

    def reset_stage_context(self, stage: str) -> None:
        """Reset logging context for a stage.

        This method resets the logging context to the specified stage,
        clearing any transient state.

        Parameters
        ----------
        stage:
            The stage name (e.g., "extract", "transform", "validate", "write").
        """
        UnifiedLogger.bind(stage=stage, component=self._component_for_stage(stage))

    def init_chembl_client(self, *, base_url: str | None = None) -> UnifiedAPIClient:
        """Initialize and register a ChEMBL API client.

        This method creates a ChEMBL client using the unified HTTP client
        with default retry/backoff, throttling, and observability policies.
        The client is automatically registered for cleanup during pipeline finalization.

        Parameters
        ----------
        base_url:
            Optional base URL for the ChEMBL API. If not provided, it will
            be resolved from the source configuration.

        Returns
        -------
        UnifiedAPIClient:
            The configured ChEMBL API client.
        """
        factory = APIClientFactory(self.config)
        source_config = self.config.sources.get("chembl")
        if source_config is None:
            msg = "ChEMBL source configuration not found"
            raise ValueError(msg)

        # Resolve base URL from source config if not provided
        if base_url is None:
            parameters = source_config.parameters or {}
            base_url = parameters.get("base_url", "https://www.ebi.ac.uk/chembl/api/data")

        # Ensure base_url is str (guaranteed by .get() with default, but type checker doesn't know)
        if not isinstance(base_url, str):
            msg = "base_url must be a string"
            raise TypeError(msg)

        client = factory.for_source("chembl", base_url=base_url)
        self.register_client("chembl_client", client)

        log = UnifiedLogger.get(__name__)
        log.debug("chembl_client_initialized", base_url=base_url)
        return client

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _component_for_stage(self, stage: str) -> str:
        return f"{self.pipeline_code}.{stage}"

    def _safe_len(self, payload: object) -> int | None:
        if isinstance(payload, pd.DataFrame):
            return int(payload.shape[0])
        if isinstance(payload, Sized):
            try:
                return int(len(payload))
            except TypeError:
                return None
        return None

    def _schema_column_specs(self) -> Mapping[str, Mapping[str, Any]]:
        """Override to provide custom defaults or dtypes for schema columns."""

        return {}

    def _ensure_schema_columns(
        self, df: pd.DataFrame, column_order: Sequence[str], log: Any
    ) -> pd.DataFrame:
        """Add missing schema columns so downstream normalization can operate safely.

        Parameters
        ----------
        df
            DataFrame to ensure columns for.
        column_order
            Sequence of column names that should exist in the DataFrame.
        log
            Logger instance for logging.

        Returns
        -------
        pd.DataFrame
            DataFrame with all schema columns present (missing ones filled with NA).
        """
        df = df.copy()
        if df.empty:
            return df

        expected = list(column_order)
        missing = [column for column in expected if column not in df.columns]
        if missing:
            specs = self._schema_column_specs()
            row_count = len(df)
            for column in missing:
                spec = specs.get(column, {})
                factory = spec.get("factory")
                if callable(factory):
                    produced = factory(row_count)
                    if isinstance(produced, pd.Series):
                        produced_series = cast(pd.Series[Any], produced)
                        if len(produced_series) == row_count:
                            df[column] = produced_series
                            continue
                default_value = spec.get("default", pd.NA)
                dtype = spec.get("dtype")
                if dtype is not None:
                    df[column] = pd.Series([default_value] * row_count, dtype=dtype)
                else:
                    df[column] = pd.Series([default_value] * row_count)
            log.debug("schema_columns_added", columns=missing)

        return df

    def _order_schema_columns(self, df: pd.DataFrame, column_order: Sequence[str]) -> pd.DataFrame:
        """Return DataFrame with schema columns ordered ahead of extras.

        Parameters
        ----------
        df
            DataFrame to reorder.
        column_order
            Sequence of column names defining the desired order.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns ordered according to column_order, with extras appended.
        """
        extras = [column for column in df.columns if column not in column_order]
        # Filter column_order to only include columns that exist in DataFrame
        existing_schema_columns = [column for column in column_order if column in df.columns]
        if self.config.validation.strict:
            # Only return schema columns that exist
            return df[existing_schema_columns]
        # Return schema columns first, then extras
        return df[[*existing_schema_columns, *extras]]

    def _normalize_data_types(
        self, df: pd.DataFrame, schema: DataFrameSchema, log: Any
    ) -> pd.DataFrame:
        """Convert data types according to the schema.

        This is a basic implementation that handles common cases. Pipelines may
        override this method for schema-specific normalization logic.

        Parameters
        ----------
        df
            DataFrame to normalize.
        schema
            Pandera DataFrameSchema defining expected types.
        log
            Logger instance for logging.

        Returns
        -------
        pd.DataFrame
            DataFrame with normalized data types.
        """
        df = df.copy()

        def _to_numeric_series(series: pd.Series[Any]) -> pd.Series[Any]:
            to_numeric_series = cast(Callable[..., pd.Series[Any]], pd.to_numeric)
            return to_numeric_series(series, errors="coerce")

        # Get column definitions from schema
        for column_name, column_def in schema.columns.items():
            if column_name not in df.columns:
                continue

            try:
                column_series = cast(pd.Series[Any], df[column_name])

                # Handle nullable integers
                if column_def.dtype == pd.Int64Dtype() or (
                    hasattr(column_def.dtype, "name") and column_def.dtype.name == "Int64"
                ):
                    numeric_series = _to_numeric_series(column_series)
                    df[column_name] = cast(pd.Series[Any], numeric_series.astype("Int64"))
                # Handle nullable floats
                elif hasattr(column_def.dtype, "name") and column_def.dtype.name == "Float64":
                    numeric_series_float = _to_numeric_series(column_series)
                    df[column_name] = cast(pd.Series[Any], numeric_series_float.astype("Float64"))
                # Handle strings - convert to string, preserving None values
                elif hasattr(column_def.dtype, "name") and column_def.dtype.name == "string":
                    mask: pd.Series[bool] = column_series.notna()
                    if bool(mask.any()):
                        df.loc[mask, column_name] = df.loc[mask, column_name].astype(str)
            except (ValueError, TypeError) as exc:
                log.warning("type_conversion_failed", field=column_name, error=str(exc))

        return df

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

    def write(
        self,
        df: pd.DataFrame,
        output_path: Path,
        *,
        extended: bool = False,
        include_correlation: bool | None = None,
        include_qc_metrics: bool | None = None,
    ) -> RunResult:
        """Write the DataFrame and all metadata artifacts to the output path.

        This method writes the dataset, quality reports, metadata, and other
        artifacts to the specified output path. According to documentation,
        this method should return RunResult, not WriteResult.

        Parameters
        ----------
        df:
            The DataFrame to write.
        output_path:
            The base output path for all artifacts.
        extended:
            Whether to include extended QC artifacts.

        Returns
        -------
        RunResult:
            All artifacts generated by the write operation.
        """
        log = UnifiedLogger.get(__name__)

        run_tag = self._normalise_run_tag(None)
        effective_extended = bool(extended or getattr(self.config.cli, "extended", False))
        mode = "extended" if effective_extended else None

        postprocess_config = getattr(self.config, "postprocess", None)
        correlation_config = getattr(postprocess_config, "correlation", None)
        correlation_default = bool(getattr(correlation_config, "enabled", False))

        include_correlation_flag = (
            bool(include_correlation)
            if include_correlation is not None
            else (effective_extended or correlation_default)
        )
        include_qc_metrics_flag = (
            bool(include_qc_metrics)
            if include_qc_metrics is not None
            else effective_extended
        )
        include_metadata = True  # Всегда создавать meta.yaml
        include_manifest = effective_extended

        run_dir = output_path if output_path.is_dir() else output_path.parent
        run_dir.mkdir(parents=True, exist_ok=True)

        artifacts = self.plan_run_artifacts(
            run_tag=run_tag,
            mode=mode,
            include_correlation=include_correlation_flag,
            include_qc_metrics=include_qc_metrics_flag,
            include_metadata=include_metadata,
            include_manifest=include_manifest,
            extras=None,
            run_directory=run_dir,
        )

        # Build write artifacts
        prepared: DeterministicWriteArtifacts = build_write_artifacts(
            df,
            config=self.config,
            run_id=self.run_id,
            pipeline_code=self.pipeline_code,
            dataset_path=artifacts.write.dataset,
            stage_durations_ms=self._stage_durations_ms,
        )

        metadata_payload = self.augment_metadata(prepared.metadata, prepared.dataframe)
        metadata = dict(metadata_payload)

        metrics_payload = self.build_qc_metrics(prepared.dataframe)
        metrics_summary: dict[str, Any] | None = None
        if isinstance(metrics_payload, Mapping):
            metrics_summary = dict(metrics_payload)
        elif isinstance(metrics_payload, pd.DataFrame):
            metrics_dict_result: dict[Any, dict[str, Any]] = metrics_payload.to_dict(orient="index")  # type: ignore[assignment]
            metrics_summary = {
                str(row.get("metric", index)): row.get("value")
                for index, row in metrics_dict_result.items()
            }

        if self._validation_summary:
            validation_default: dict[str, Any] = {}
            validation_dict = cast(
                dict[str, Any], metadata.setdefault("validation", validation_default)
            )
            validation_dict.update(self._validation_summary)

        if metrics_summary:
            quality_default: dict[str, Any] = {}
            quality_dict = cast(dict[str, Any], metadata.setdefault("quality", quality_default))
            metrics_default: dict[str, Any] = {}
            metrics_dict = cast(dict[str, Any], quality_dict.setdefault("metrics", metrics_default))
            metrics_dict.update(metrics_summary)

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

        # Create WriteResult for RunResult
        write_result = WriteResult(
            dataset=artifacts.write.dataset,
            metadata=metadata_path,
            quality_report=quality_path,
            correlation_report=correlation_path,
            qc_metrics=metrics_path,
            extras=dict(artifacts.extras),
        )

        # Return RunResult according to documentation
        return RunResult(
            write_result=write_result,
            run_directory=artifacts.run_directory,
            manifest=artifacts.manifest,
            additional_datasets=dict(artifacts.extras),
            qc_summary=metrics_path,
            debug_dataset=None,  # Not implemented yet
            run_id=self.run_id,
            log_file=artifacts.log_file,
            stage_durations_ms=self._stage_durations_ms,
        )

    def run(
        self,
        output_path: Path,
        *args: object,
        extended: bool = False,
        include_correlation: bool | None = None,
        include_qc_metrics: bool | None = None,
        **kwargs: object,
    ) -> RunResult:
        """Execute the pipeline lifecycle and return collected artifacts.

        According to documentation, this method should accept output_path as the
        first positional argument after self.

        Parameters
        ----------
        output_path:
            The base output path for all artifacts.
        extended:
            Whether to include extended QC artifacts.
        *args:
            Additional positional arguments passed to extract().
        **kwargs:
            Additional keyword arguments passed to extract().

        Returns
        -------
        RunResult:
            All artifacts generated by the pipeline run.
        """
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
        self._extract_metadata = {}

        effective_extended = bool(extended or getattr(self.config.cli, "extended", False))
        configured_mode = "extended" if effective_extended else None

        postprocess_config = getattr(self.config, "postprocess", None)
        correlation_config = getattr(postprocess_config, "correlation", None)
        correlation_default = bool(getattr(correlation_config, "enabled", False))
        include_correlation_flag = (
            bool(include_correlation)
            if include_correlation is not None
            else (effective_extended or correlation_default)
        )
        include_qc_metrics_flag = (
            bool(include_qc_metrics)
            if include_qc_metrics is not None
            else effective_extended
        )

        UnifiedLogger.bind(stage="bootstrap")
        log.info("pipeline_started", mode=configured_mode, output_path=str(output_path))

        try:
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

            # transformed is always pd.DataFrame according to transform signature
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
                log.info("write_started", output_path=str(output_path))
                write_start = time.perf_counter()
                result = self.write(
                    validated,
                    output_path,
                    extended=effective_extended,
                    include_correlation=include_correlation_flag,
                    include_qc_metrics=include_qc_metrics_flag,
                )
                duration = (time.perf_counter() - write_start) * 1000.0
                stage_durations_ms["write"] = duration
                log.info(
                    "write_completed",
                    duration_ms=duration,
                    dataset=str(result.write_result.dataset),
                )

            self.apply_retention_policy()
            log.info("pipeline_completed", stage_durations_ms=stage_durations_ms)

            return result

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

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate ``df`` against the configured Pandera schema.

        According to documentation, this method should accept df: pd.DataFrame.
        """
        schema_identifier = self.config.validation.schema_out
        log = UnifiedLogger.get(__name__)
        if not schema_identifier:
            log.debug("validation_skipped", reason="no_schema_configured")
            self._validation_schema = None
            self._validation_summary = None
            return df

        schema_entry = get_schema(schema_identifier)
        schema = schema_entry.schema
        if hasattr(schema, "replace") and callable(getattr(schema, "replace", None)):
            schema = cast(Any, schema).replace(
                strict=self.config.validation.strict,
                coerce=self.config.validation.coerce,
            )
        else:
            schema = self._clone_schema_with_options(
                schema,
                strict=self.config.validation.strict,
                coerce=self.config.validation.coerce,
            )
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

        df_for_validation = ensure_hash_columns(df, config=self.config)

        try:
            validated = schema.validate(df_for_validation, lazy=True)
            if not isinstance(validated, pd.DataFrame):
                msg = "Schema validation did not return a DataFrame"
                raise TypeError(msg)
            validated = self._reorder_columns(validated, schema_entry.column_order)
        except pandera.errors.SchemaErrors as exc:
            if not fail_open:
                raise
            summary = summarize_schema_errors(exc)
            failure_cases_df = getattr(exc, "failure_cases", None)
            failure_details: dict[str, Any] | None = None
            if isinstance(failure_cases_df, pd.DataFrame) and not failure_cases_df.empty:
                failure_details = format_failure_cases(failure_cases_df)
                summary["failure_count"] = int(len(failure_cases_df))
            log_payload: dict[str, Any] = {
                "schema": schema_entry.identifier,
                "version": schema_entry.version,
                **summary,
            }
            if failure_details:
                log_payload["failure_details"] = failure_details
            log.warning("schema_validation_failed", **log_payload)
            validated = df_for_validation
            schema_valid = False
            error_summary = summary.get("message")
            failure_count = summary.get("failure_count")

        self._validation_schema = schema_entry
        self._validation_summary = {
            "schema_identifier": schema_entry.identifier,
            "schema_name": schema_entry.name,
            "schema_version": schema_entry.version,
            "column_order": list(schema_entry.column_order),
            "strict": bool(self.config.validation.strict),
            "coerce": bool(self.config.validation.coerce),
            "row_count": int(len(validated)),
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

    @staticmethod
    def _clone_schema_with_options(
        schema: Any,
        *,
        strict: bool | None,
        coerce: bool | None,
    ) -> Any:
        """Clone ``schema`` applying strict/coerce flags for DataFrameSchema compatibility."""

        if schema.__class__.__name__ != "DataFrameSchema":
            return schema

        schema_cls = schema.__class__
        return schema_cls(
            schema.columns,
            checks=schema.checks,
            index=schema.index,
            dtype=schema.dtype,
            coerce=schema.coerce if coerce is None else coerce,
            strict=schema.strict if strict is None else strict,
            name=schema.name,
            ordered=schema.ordered,
            unique=schema.unique,
            report_duplicates=schema.report_duplicates,
            unique_column_names=schema.unique_column_names,
            add_missing_columns=schema.add_missing_columns,
            drop_invalid_rows=schema.drop_invalid_rows,
            title=schema.title,
            description=schema.description,
            metadata=schema.metadata,
            parsers=schema.parsers,
        )

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
