"""UnifiedOutputWriter: deterministic data writing with quality metrics and atomic writes."""

from __future__ import annotations

import hashlib
import json
import os
from contextvars import ContextVar

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from collections.abc import Callable, Sequence
from typing import Any, TYPE_CHECKING

import pandas as pd

from bioetl.core.logger import UnifiedLogger
from bioetl.config.models import DeterminismConfig

if TYPE_CHECKING:  # pragma: no cover - assists static analysers only.
    from bioetl.config import PipelineConfig
    from bioetl.schemas.base import BaseSchema

logger = UnifiedLogger.get(__name__)


_ATOMIC_TEMP_DIR_NAME: ContextVar[str] = ContextVar(
    "_atomic_temp_dir_name",
    default=".tmp_atomic",
)
_ACTIVE_ATOMIC_TEMP_PATH: ContextVar[Path] = ContextVar("_active_atomic_temp_path")


def _get_active_atomic_temp_path() -> Path:
    """Return the temporary path registered for the current atomic write."""

    try:
        return _ACTIVE_ATOMIC_TEMP_PATH.get()
    except LookupError as exc:  # pragma: no cover - defensive guard
        raise RuntimeError("_atomic_write called without an active temp path") from exc


def _cleanup_temp_dir(temp_dir: Path) -> None:
    """Remove temporary directory if empty and cleanup stray ``*.tmp`` files."""

    try:
        if not temp_dir.exists():
            return

        if any(temp_dir.iterdir()):
            for temp_file in temp_dir.glob("*.tmp"):
                temp_file.unlink(missing_ok=True)
            if any(temp_dir.iterdir()):
                return
        temp_dir.rmdir()
    except OSError:
        # Best-effort cleanup – temp artefacts are safe to leave behind if removal fails.
        pass


def _atomic_write(path: Path, write_fn: Callable[[], None]) -> None:
    """Execute ``write_fn`` within an atomic file replacement workflow."""

    temp_dir_name = _ATOMIC_TEMP_DIR_NAME.get()
    temp_dir = path.parent / temp_dir_name
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_path = temp_dir / f"{path.name}.tmp"

    token = _ACTIVE_ATOMIC_TEMP_PATH.set(temp_path)
    try:
        write_fn()
        path.parent.mkdir(parents=True, exist_ok=True)
        os.replace(str(temp_path), str(path))
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
    finally:
        _ACTIVE_ATOMIC_TEMP_PATH.reset(token)
        _cleanup_temp_dir(temp_dir)


@dataclass(frozen=True)
class TableArtifacts:
    """Артефакты, связанные с единичной таблицей (dataset + QC + metadata)."""

    datasets: dict[str, Path]
    quality_report: Path
    metadata: Path


@dataclass(frozen=True)
class OutputArtifacts:
    """Пути к стандартным выходным артефактам."""

    dataset: Path
    quality_report: Path
    run_directory: Path
    additional_datasets: dict[str, TableArtifacts] = field(default_factory=dict)
    dataset_formats: dict[str, Path] = field(default_factory=dict)
    correlation_report: Path | None = None
    metadata: Path | None = None
    manifest: Path | None = None
    qc_summary: Path | None = None
    qc_missing_mappings: Path | None = None
    qc_enrichment_metrics: Path | None = None
    qc_summary_statistics: Path | None = None
    qc_dataset_metrics: Path | None = None
    debug_dataset: Path | None = None


@dataclass(frozen=True)
class AdditionalTableSpec:
    """Описание дополнительной таблицы для экспорта."""

    dataframe: pd.DataFrame
    relative_path: Path | None = None
    formats: tuple[str, ...] = ("csv",)
    sort_by: tuple[str, ...] | None = None
    sort_ascending: tuple[bool, ...] | None = None
    column_order: tuple[str, ...] | None = None


@dataclass(frozen=True)
class OutputMetadata:
    """Метаданные выходного файла."""

    pipeline_version: str
    source_system: str
    chembl_release: str | None
    generated_at: str  # UTC ISO8601
    row_count: int
    column_count: int
    column_order: list[str]
    checksums: dict[str, str]
    run_id: str | None = None
    config_hash: str | None = None
    git_commit: str | None = None
    sources: tuple[str, ...] = field(default_factory=tuple)
 
    schema_id: str | None = None
    schema_version: str | None = None
    column_order_source: str | None = None
    na_policy: str | None = None
    precision_policy: str | None = None
    hash_policy_version: str | None = None

    @classmethod
    def from_dataframe(
        cls,
        df: pd.DataFrame,
        pipeline_version: str = "1.0.0",
        source_system: str = "unified",
        chembl_release: str | None = None,
        column_order: list[str] | None = None,
        run_id: str | None = None,
        *,
        config_hash: str | None = None,
        git_commit: str | None = None,
        sources: Sequence[str] | None = None,
        schema: type[BaseSchema] | None = None,
        hash_policy_version: str | None = None,
    ) -> "OutputMetadata":
        """Создает метаданные из DataFrame."""

        normalised_sources: tuple[str, ...] = ()
        if sources:
            seen: set[str] = set()
            ordered: list[str] = []
            for source in sources:
                if not source:
                    continue
                key = str(source)
                if key in seen:
                    continue
                seen.add(key)
                ordered.append(key)
            normalised_sources = tuple(ordered)

        schema_id: str | None = None
        schema_version: str | None = None
        column_order_source = "dataframe"
        na_policy: str | None = None
        precision_policy: str | None = None

        if schema is not None:
            from bioetl.schemas.registry import SchemaRegistry

            if not isinstance(schema, type):
                schema_cls = type(schema)
            else:
                schema_cls = schema

            registration = SchemaRegistry.find_registration(schema_cls)
            if registration is not None:
                schema_id = registration.schema_id
                schema_version = registration.version
                column_order_source = registration.column_order_source
                na_policy = registration.na_policy
                precision_policy = registration.precision_policy
            else:
                schema_id = getattr(schema_cls, "schema_id", None)
                schema_version = getattr(schema_cls, "schema_version", None)
                column_order_source = "schema"
                na_policy = getattr(schema_cls, "na_policy", None)
                precision_policy = getattr(schema_cls, "precision_policy", None)

        return cls(
            pipeline_version=pipeline_version,
            source_system=source_system,
            chembl_release=chembl_release,
            generated_at=datetime.now(timezone.utc).isoformat(),
            row_count=len(df),
            column_count=len(df.columns),
            column_order=column_order or list(df.columns),
            checksums={},
            run_id=run_id,
            config_hash=config_hash,
            git_commit=git_commit,
            sources=normalised_sources,

            schema_id=schema_id,
            schema_version=schema_version,
            column_order_source=column_order_source,
            na_policy=na_policy,
            precision_policy=precision_policy,
            hash_policy_version=hash_policy_version,
        )


def _resolve_float_format(
    determinism: DeterminismConfig | None,
    override: str | None,
) -> str | None:
    """Return the float format string derived from determinism settings."""

    if override is not None:
        return override

    if determinism is None:
        return None

    precision = determinism.float_precision
    return f"%.{precision}f"


def _resolve_date_format(
    determinism: DeterminismConfig | None,
    override: str | None,
) -> str | None:
    """Return the date format string respecting ``iso8601`` default semantics."""

    if override is not None:
        return override

    if determinism is None:
        return None

    fmt = determinism.datetime_format
    if fmt.lower() == "iso8601":
        return None

    return fmt


class AtomicWriter:
    """Атомарная запись с защитой от corruption."""

    def __init__(self, run_id: str, determinism: DeterminismConfig | None = None):
        self.run_id = run_id
        self.determinism = determinism or DeterminismConfig()

    def write(self, data: pd.DataFrame, path: Path, **kwargs) -> None:
        """Записывает data в path атомарно через run-scoped temp directory."""
        temp_dir_token = _ATOMIC_TEMP_DIR_NAME.set(f".tmp_run_{self.run_id}")

        resolved_float_format = _resolve_float_format(
            self.determinism, kwargs.pop("float_format", None)
        )
        resolved_date_format = _resolve_date_format(
            self.determinism, kwargs.pop("date_format", None)
        )
        target_suffix = path.suffix.lower()

        def write_payload() -> None:
            temp_path = _get_active_atomic_temp_path()
            self._write_to_file(
                data,
                temp_path,
                float_format=resolved_float_format,
                date_format=resolved_date_format,
                target_suffix=target_suffix,
                **kwargs,
            )

        try:
            _atomic_write(path, write_payload)
        finally:
            _ATOMIC_TEMP_DIR_NAME.reset(temp_dir_token)

    def _write_to_file(
        self,
        data: pd.DataFrame,
        path: Path,
        *,
        float_format: str | None = None,
        date_format: str | None = None,
        **kwargs,
    ) -> None:
        """Записывает DataFrame в файл."""
        suffix = kwargs.pop("target_suffix", path.suffix.lower())
        if suffix == ".parquet":
            parquet_kwargs = kwargs.copy()
            parquet_kwargs.pop("float_format", None)
            parquet_kwargs.pop("date_format", None)
            data.to_parquet(path, index=False, **parquet_kwargs)
            return

        if suffix not in {".csv", ""}:
            raise ValueError(f"Unsupported file format for atomic write: {suffix}")

        data.to_csv(
            path,
            index=False,
            float_format=float_format,
            date_format=date_format,
            **kwargs,
        )


class QualityReportGenerator:
    """Генератор quality report."""

    def generate(
        self,
        df: pd.DataFrame,
        issues: list[dict[str, Any]] | None = None,
        qc_metrics: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Создает QC отчет."""
        rows: list[dict[str, Any]] = []

        for column in df.columns:
            null_count = df[column].isna().sum()
            null_fraction = null_count / len(df) if len(df) > 0 else 0
            unique_count = df[column].nunique()

            rows.append(
                {
                    "metric": "column_profile",
                    "column": column,
                    "null_count": null_count,
                    "null_fraction": null_fraction,
                    "unique_count": unique_count,
                    "dtype": str(df[column].dtype),
                }
            )

        if issues:
            for issue in issues:
                record = {"metric": issue.get("metric", "validation_issue")}
                record.update(issue)
                rows.append(record)

        if qc_metrics:
            for name, value in qc_metrics.items():
                entry: dict[str, Any] = {
                    "metric": "qc_metric",
                    "name": name,
                    "value": value,
                }
                rows.append(entry)

        return pd.DataFrame(rows)


class UnifiedOutputWriter:
    """Unified output writer with atomic writes and QC reports."""

    def __init__(
        self,
        run_id: str,
        determinism: DeterminismConfig | None = None,
        pipeline_config: "PipelineConfig" | None = None,
    ):
        self.run_id = run_id
        self.determinism = determinism or DeterminismConfig()
        self.atomic_writer = AtomicWriter(run_id, determinism=self.determinism)
        self.quality_generator = QualityReportGenerator()
        self.pipeline_config: "PipelineConfig" | None = pipeline_config

    @staticmethod
    def _extension_for_format(fmt: str) -> str:
        if fmt == "csv":
            return ".csv"
        if fmt == "parquet":
            return ".parquet"
        raise ValueError(f"Unsupported dataset format: {fmt}")

    def _normalize_formats(
        self, formats: Sequence[str] | None, default_path: Path
    ) -> list[str]:
        """Return a deterministic list of dataset formats to materialise."""

        allowed = {"csv", "parquet"}
        normalised: list[str] = []

        if formats:
            candidates = formats
        else:
            suffix = default_path.suffix.lower()
            if suffix in {".csv", ".parquet"}:
                candidates = (suffix.lstrip("."),)
            else:
                candidates = ("csv",)

        for value in candidates:
            key = str(value).lower()
            if key not in allowed:
                logger.warning("unsupported_dataset_format", format=value)
                continue
            if key not in normalised:
                normalised.append(key)

        if not normalised:
            normalised.append("csv")

        return normalised

    def _apply_column_order(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a dataframe matching the configured deterministic column order."""

        configured_order = [column for column in self.determinism.column_order if column]
        if not configured_order:
            return df

        missing_columns = [column for column in configured_order if column not in df.columns]
        extra_columns = [column for column in df.columns if column not in configured_order]

        if not missing_columns and not extra_columns and list(df.columns) == configured_order:
            return df

        ordered = df.copy()
        for column in missing_columns:
            ordered[column] = pd.NA

        remaining_columns = [column for column in ordered.columns if column not in configured_order]
        return ordered[configured_order + remaining_columns]

    @staticmethod
    def _apply_custom_column_order(
        df: pd.DataFrame, column_order: Sequence[str] | None
    ) -> pd.DataFrame:
        if not column_order:
            return df

        ordered = df.copy()
        missing_columns = [column for column in column_order if column not in ordered.columns]
        for column in missing_columns:
            ordered[column] = pd.NA

        remaining = [column for column in ordered.columns if column not in column_order]
        return ordered[list(column_order) + remaining]

    @staticmethod
    def _align_metadata_with_dataframe(
        metadata: OutputMetadata,
        df: pd.DataFrame,
    ) -> OutputMetadata:
        updates: dict[str, Any] = {}
        column_order = list(df.columns)
        if metadata.column_order != column_order:
            updates["column_order"] = column_order
        if metadata.column_count != len(column_order):
            updates["column_count"] = len(column_order)
        if metadata.row_count != len(df):
            updates["row_count"] = len(df)

        if updates:
            metadata = replace(metadata, **updates)
        return metadata

    def write_table_bundle(
        self,
        df: pd.DataFrame,
        output_path: Path,
        *,
        formats: Sequence[str] | None = None,
        metadata: OutputMetadata | None = None,
        issues: list[dict[str, Any]] | None = None,
        qc_metrics: dict[str, Any] | None = None,
        column_order: Sequence[str] | None = None,
        sort_by: Sequence[str] | None = None,
        sort_ascending: Sequence[bool] | None = None,
        qc_directory: Path | None = None,
        runtime_options: dict[str, Any] | None = None,
    ) -> TableArtifacts:
        """Записывает таблицу в один или несколько форматов с QC и метаданными."""

        output_path = Path(output_path)
        resolved_formats = self._normalize_formats(formats, output_path)

        if output_path.suffix:
            base_dir = output_path.parent
            file_stem = output_path.stem
        else:
            base_dir = output_path
            file_stem = output_path.name

        base_dir.mkdir(parents=True, exist_ok=True)

        processed = df.copy()
        if column_order:
            processed = self._apply_custom_column_order(processed, column_order)

        if sort_by:
            resolved_ascending: list[bool] = []
            ascending_seq = list(sort_ascending or [])
            for idx, column in enumerate(sort_by):
                if column not in processed.columns:
                    continue
                resolved_ascending.append(
                    ascending_seq[idx] if idx < len(ascending_seq) else (ascending_seq[-1] if ascending_seq else True)
                )
            valid_columns = [column for column in sort_by if column in processed.columns]
            if valid_columns:
                processed = processed.sort_values(
                    by=valid_columns,
                    ascending=resolved_ascending or True,
                    kind="stable",
                )

        processed = processed.reset_index(drop=True)

        float_precision = self.determinism.float_precision
        float_format = f"%.{float_precision}f"

        dataset_paths: dict[str, Path] = {}
        for fmt in resolved_formats:
            target_path = base_dir / f"{file_stem}{self._extension_for_format(fmt)}"
            logger.info(
                "writing_table_dataset",
                path=str(target_path),
                format=fmt,
                rows=len(processed),
            )
            self.atomic_writer.write(processed, target_path, float_format=float_format)
            dataset_paths[fmt] = target_path

        resolved_qc_dir = qc_directory or (base_dir / "qc")
        resolved_qc_dir.mkdir(parents=True, exist_ok=True)
        quality_path = resolved_qc_dir / f"{file_stem}_quality_report.csv"
        quality_df = self.quality_generator.generate(
            processed,
            issues=issues,
            qc_metrics=qc_metrics,
        )
        logger.info(
            "writing_table_quality_report",
            path=str(quality_path),
            rows=len(quality_df),
        )
        self.atomic_writer.write(quality_df, quality_path, float_format=float_format)

        if metadata is None:
            metadata = OutputMetadata.from_dataframe(
                processed,
                run_id=self.run_id,
                column_order=list(processed.columns),
            )
        else:
            metadata = self._align_metadata_with_dataframe(metadata, processed)
            if metadata.run_id is None:
                metadata = replace(metadata, run_id=self.run_id)

        checksums = self._calculate_checksums(*dataset_paths.values(), quality_path)
        metadata_path = base_dir / f"{file_stem}_meta.yaml"

        self._write_metadata(
            metadata_path,
            metadata,
            checksums,
            dataset_path=dataset_paths,
            quality_path=quality_path,
            runtime_options=runtime_options,
            qc_metrics=qc_metrics,
            issues=issues,
        )

        return TableArtifacts(
            datasets=dataset_paths,
            quality_report=quality_path,
            metadata=metadata_path,
        )

    def write(
        self,
        df: pd.DataFrame,
        output_path: Path,
        metadata: OutputMetadata | None = None,
        extended: bool = False,
        issues: list[dict[str, Any]] | None = None,
        qc_metrics: dict[str, Any] | None = None,
        qc_summary: dict[str, Any] | None = None,
        qc_missing_mappings: pd.DataFrame | None = None,
        qc_enrichment_metrics: pd.DataFrame | None = None,
        additional_tables: dict[str, "AdditionalTableSpec"] | None = None,
        runtime_options: dict[str, Any] | None = None,
        debug_dataset: Path | None = None,
        formats: Sequence[str] | None = None,
    ) -> OutputArtifacts:
        """
        Записывает DataFrame с QC отчетами и метаданными.

        Args:
            df: DataFrame для записи
            output_path: Путь к основному файлу
            metadata: Метаданные (если None, создаются автоматически)
            extended: Включать ли расширенные артефакты

        Returns:
            OutputArtifacts с путями к созданным файлам
        """
        dataset_df = self._apply_column_order(df)
        column_order = list(dataset_df.columns)
        float_precision = self.determinism.float_precision
        float_format = f"%.{float_precision}f"

        resolved_formats = self._normalize_formats(formats, output_path)

        if output_path.suffix:
            dataset_dir = output_path.parent
            dataset_stem = output_path.stem
        else:
            dataset_dir = output_path
            dataset_stem = output_path.name

        dataset_dir.mkdir(parents=True, exist_ok=True)

        run_directory = dataset_dir
        if dataset_dir.name.lower() in {"datasets", "tables", "csv"}:
            run_directory = dataset_dir.parent

        qc_dir = run_directory / "qc"
        qc_dir.mkdir(parents=True, exist_ok=True)

        dataset_paths: dict[str, Path] = {}
        for fmt in resolved_formats:
            candidate_path = dataset_dir / f"{dataset_stem}{self._extension_for_format(fmt)}"
            dataset_paths[fmt] = candidate_path
            logger.info(
                "writing_dataset",
                path=str(candidate_path),
                format=fmt,
                rows=len(dataset_df),
                run_directory=str(run_directory),
            )
            self.atomic_writer.write(dataset_df, candidate_path, float_format=float_format)

        primary_format = "csv" if "csv" in dataset_paths else resolved_formats[0]
        dataset_path = dataset_paths[primary_format]

        quality_path = qc_dir / f"{dataset_stem}_quality_report.csv"

        # Generate metadata if not provided
        if metadata is None:
            metadata = OutputMetadata.from_dataframe(
                dataset_df,
                run_id=self.run_id,
                column_order=column_order,
            )
        else:
            metadata_updates: dict[str, Any] = {}
            if metadata.run_id is None:
                metadata_updates["run_id"] = self.run_id
            if metadata.column_order != column_order:
                metadata_updates["column_order"] = column_order
            if metadata.column_count != len(column_order):
                metadata_updates["column_count"] = len(column_order)
            if metadata.row_count != len(dataset_df):
                metadata_updates["row_count"] = len(dataset_df)

            if metadata_updates:
                metadata = replace(metadata, **metadata_updates)

        logger.info(
            "generating_quality_report",
            path=str(quality_path),
            rows=len(dataset_df.columns),
        )
        quality_df = self.quality_generator.generate(
            dataset_df,
            issues=issues,
            qc_metrics=qc_metrics,
        )
        self.atomic_writer.write(quality_df, quality_path, float_format=float_format)

        qc_summary_path: Path | None = None
        if qc_summary:
            qc_summary_path = qc_dir / "qc_summary.json"
            logger.info("writing_qc_summary", path=str(qc_summary_path))
            self._write_json_atomic(qc_summary_path, qc_summary)

        missing_mappings_path: Path | None = None
        if qc_missing_mappings is not None and not qc_missing_mappings.empty:
            missing_mappings_path = qc_dir / "qc_missing_mappings.csv"
            logger.info(
                "writing_qc_missing_mappings",
                path=str(missing_mappings_path),
                rows=len(qc_missing_mappings),
            )
            self.atomic_writer.write(
                qc_missing_mappings,
                missing_mappings_path,
                float_format=float_format,
            )

        enrichment_metrics_path: Path | None = None
        if qc_enrichment_metrics is not None and not qc_enrichment_metrics.empty:
            enrichment_metrics_path = qc_dir / "qc_enrichment_metrics.csv"
            logger.info(
                "writing_qc_enrichment_metrics",
                path=str(enrichment_metrics_path),
                rows=len(qc_enrichment_metrics),
            )
            self.atomic_writer.write(
                qc_enrichment_metrics,
                enrichment_metrics_path,
                float_format=float_format,
            )

        additional_artifacts: dict[str, TableArtifacts] = {}
        if additional_tables:
            for name, table_spec in additional_tables.items():
                if table_spec is None:
                    continue

                table = table_spec.dataframe
                if table is None or table.empty:
                    continue

                table_relative_path = table_spec.relative_path
                if table_relative_path is not None:
                    table_output = run_directory / table_relative_path
                else:
                    safe_name = name.replace(" ", "_").lower()
                    table_output = dataset_dir / f"{safe_name}.csv"

                bundle = self.write_table_bundle(
                    table,
                    table_output,
                    formats=table_spec.formats,
                    column_order=table_spec.column_order,
                    sort_by=table_spec.sort_by,
                    sort_ascending=table_spec.sort_ascending,
                    qc_directory=qc_dir,
                    runtime_options=runtime_options,
                )
                additional_artifacts[name] = bundle

        correlation_path: Path | None = None
        summary_statistics_path: Path | None = None
        dataset_metrics_path: Path | None = None

        if extended:
            correlation_df = self._build_correlation_report(dataset_df)
            if correlation_df is not None and not correlation_df.empty:
                correlation_path = qc_dir / f"{dataset_path.stem}_correlation_report.csv"
                logger.info(
                    "writing_correlation_report",
                    path=str(correlation_path),
                    rows=len(correlation_df),
                )
                self.atomic_writer.write(
                    correlation_df,
                    correlation_path,
                    float_format=float_format,
                )
            else:
                logger.info(
                    "skip_correlation_report",
                    reason="no_numeric_columns" if correlation_df is None else "empty_payload",
                )

            summary_statistics_df = self._build_summary_statistics(dataset_df)
            if summary_statistics_df is not None and not summary_statistics_df.empty:
                summary_statistics_path = (
                    qc_dir / f"{dataset_path.stem}_summary_statistics.csv"
                )
                logger.info(
                    "writing_summary_statistics",
                    path=str(summary_statistics_path),
                    rows=len(summary_statistics_df),
                )
                self.atomic_writer.write(
                    summary_statistics_df,
                    summary_statistics_path,
                    float_format=float_format,
                )

            dataset_metrics_df = self._build_dataset_metrics(dataset_df)
            if dataset_metrics_df is not None and not dataset_metrics_df.empty:
                dataset_metrics_path = qc_dir / f"{dataset_path.stem}_dataset_metrics.csv"
                logger.info(
                    "writing_dataset_metrics",
                    path=str(dataset_metrics_path),
                    rows=len(dataset_metrics_df),
                )
                self.atomic_writer.write(
                    dataset_metrics_df,
                    dataset_metrics_path,
                    float_format=float_format,
                )

        checksum_targets: list[Path] = [
            *dataset_paths.values(),
            quality_path,
        ]
        optional_targets: tuple[Path | None, ...] = (
            missing_mappings_path,
            enrichment_metrics_path,
            qc_summary_path,
            correlation_path,
            summary_statistics_path,
            dataset_metrics_path,
        )
        checksum_targets.extend(path for path in optional_targets if path is not None)

        for bundle in additional_artifacts.values():
            checksum_targets.extend(bundle.datasets.values())
            checksum_targets.append(bundle.quality_report)
            checksum_targets.append(bundle.metadata)

        checksums = self._calculate_checksums(*checksum_targets)

        metadata_filename = f"{dataset_stem}_meta.yaml"
        metadata_path = run_directory / metadata_filename
        qc_artifact_paths = {
            "qc_summary": qc_summary_path,
            "qc_missing_mappings": missing_mappings_path,
            "qc_enrichment_metrics": enrichment_metrics_path,
        }
        if correlation_path is not None:
            qc_artifact_paths["correlation_report"] = correlation_path
        if summary_statistics_path is not None:
            qc_artifact_paths["summary_statistics"] = summary_statistics_path
        if dataset_metrics_path is not None:
            qc_artifact_paths["dataset_metrics"] = dataset_metrics_path

        self._write_metadata(
            metadata_path,
            metadata,
            checksums,
            dataset_path=dataset_paths,
            quality_path=quality_path,
            additional_paths=additional_artifacts,
            qc_summary=qc_summary,
            qc_metrics=qc_metrics,
            issues=issues,
            qc_artifacts=qc_artifact_paths,
            runtime_options=runtime_options,
        )

        return OutputArtifacts(
            dataset=dataset_path,
            quality_report=quality_path,
            run_directory=run_directory,
            additional_datasets=additional_artifacts,
            dataset_formats=dataset_paths,
            correlation_report=correlation_path,
            metadata=metadata_path,
            qc_summary=qc_summary_path,
            qc_missing_mappings=missing_mappings_path,
            qc_enrichment_metrics=enrichment_metrics_path,
            qc_summary_statistics=summary_statistics_path,
            qc_dataset_metrics=dataset_metrics_path,
            debug_dataset=debug_dataset,
        )

    def write_dataframe_json(
        self,
        df: pd.DataFrame,
        json_path: Path,
        *,
        orient: str = "records",
        date_format: str | None = None,
    ) -> None:
        """Serialize ``df`` to JSON using the same atomic guarantees as CSV writes."""

        logger.info(
            "writing_dataframe_json",
            path=str(json_path),
            rows=len(df),
            orient=orient,
            date_format=date_format or self.determinism.datetime_format,
        )

        dataframe_to_serialize = df
        pandas_date_format = date_format

        resolved_date_format = _resolve_date_format(
            self.determinism, date_format
        )
        datetime_columns = df.select_dtypes(include=["datetime", "datetimetz"]).columns

        if date_format is None:
            if resolved_date_format is None:
                if self.determinism.datetime_format.lower() == "iso8601" and len(
                    datetime_columns
                ) > 0:
                    dataframe_to_serialize = df.copy()
                    for column in datetime_columns:
                        dataframe_to_serialize[column] = dataframe_to_serialize[
                            column
                        ].apply(
                            lambda value: value.isoformat()
                            if pd.notna(value)
                            else None
                        )
                pandas_date_format = None
            else:
                if len(datetime_columns) > 0:
                    dataframe_to_serialize = df.copy()
                    for column in datetime_columns:
                        dataframe_to_serialize[column] = dataframe_to_serialize[
                            column
                        ].dt.strftime(resolved_date_format)
                pandas_date_format = None

        if not dataframe_to_serialize.empty:
            sorted_columns = sorted(dataframe_to_serialize.columns)
            if list(dataframe_to_serialize.columns) != sorted_columns:
                dataframe_to_serialize = dataframe_to_serialize.loc[:, sorted_columns]

        if dataframe_to_serialize.empty:
            json_payload = "[]"
        else:
            json_payload = dataframe_to_serialize.to_json(
                orient=orient,
                force_ascii=False,
                date_format=pandas_date_format,
                double_precision=self.determinism.float_precision,
                indent=2,
            )

        self._write_json_atomic(json_path, json_payload)

    def _calculate_checksums(self, *paths: Path | None) -> dict[str, str]:
        """Вычисляет checksums для файлов."""
        checksums = {}
        for path in paths:
            if path is None:
                continue
            if path.exists():
                with path.open("rb") as f:
                    digest = hashlib.sha256()
                    for chunk in iter(lambda: f.read(1024 * 1024), b""):
                        digest.update(chunk)
                    checksums[path.name] = digest.hexdigest()
        return checksums

    def _build_correlation_report(self, df: pd.DataFrame) -> pd.DataFrame | None:
        """Prepare a tidy correlation report for numeric columns."""

        if df.empty:
            return None

        numeric_df = df.select_dtypes(include="number")
        if numeric_df.empty:
            return None

        correlation = numeric_df.corr(numeric_only=True)
        if correlation.empty:
            return None

        correlation = correlation.round(6)
        tidy = (
            correlation.reset_index()
            .rename(columns={"index": "feature_x"})
            .melt(id_vars="feature_x", var_name="feature_y", value_name="correlation")
        )
        tidy = tidy.dropna(subset=["correlation"])
        tidy = tidy.sort_values(["feature_x", "feature_y"]).reset_index(drop=True)
        return tidy

    def _build_summary_statistics(self, df: pd.DataFrame) -> pd.DataFrame | None:
        """Build descriptive statistics for all columns."""

        if df.empty:
            return None

        try:
            summary = df.describe(include="all")
        except (TypeError, ValueError):
            return None

        if summary.empty:
            return None

        summary = summary.transpose().reset_index().rename(columns={"index": "column"})
        summary = summary.convert_dtypes()
        return summary

    def _build_dataset_metrics(self, df: pd.DataFrame) -> pd.DataFrame | None:
        """Compute dataset-level QC metrics."""

        row_count = int(len(df))
        column_count = int(df.shape[1])
        total_cells = row_count * column_count

        null_cells = int(df.isna().sum().sum()) if total_cells else 0
        null_fraction = (null_cells / total_cells) if total_cells else 0.0
        duplicate_rows = int(df.duplicated(keep=False).sum()) if row_count else 0
        numeric_columns = int(df.select_dtypes(include="number").shape[1])
        categorical_columns = int(
            df.select_dtypes(include=["object", "string", "category"]).shape[1]
        )
        datetime_columns = int(
            df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).shape[1]
        )
        memory_usage = int(df.memory_usage(deep=True).sum()) if column_count else 0

        metrics = [
            {"metric": "row_count", "value": row_count},
            {"metric": "column_count", "value": column_count},
            {"metric": "duplicate_rows", "value": duplicate_rows},
            {"metric": "null_cells_total", "value": null_cells},
            {"metric": "null_fraction_total", "value": null_fraction},
            {"metric": "numeric_column_count", "value": numeric_columns},
            {"metric": "categorical_column_count", "value": categorical_columns},
            {"metric": "datetime_column_count", "value": datetime_columns},
            {"metric": "memory_usage_bytes", "value": memory_usage},
        ]

        metrics_df = pd.DataFrame(metrics)
        return metrics_df.convert_dtypes()

    def _write_metadata(
        self,
        path: Path,
        metadata: OutputMetadata,
        checksums: dict[str, str],
        *,
        dataset_path: Path | dict[str, Path],
        quality_path: Path,
        additional_paths: dict[str, TableArtifacts] | None = None,
        qc_summary: dict[str, Any] | None = None,
        qc_metrics: dict[str, Any] | None = None,
        issues: list[dict[str, Any]] | None = None,
        qc_artifacts: dict[str, Path | None] | None = None,
        runtime_options: dict[str, Any] | None = None,
    ) -> None:
        """Записывает метаданные в YAML."""
        import yaml

        meta_dict: dict[str, Any] = {
            "run_id": metadata.run_id,
            "pipeline_version": metadata.pipeline_version,
            "source_system": metadata.source_system,
            "chembl_release": metadata.chembl_release,
            "extraction_timestamp": metadata.generated_at,
            "row_count": metadata.row_count,
            "column_count": metadata.column_count,
            "column_order": metadata.column_order,
            "file_checksums": checksums,
            "config_hash": metadata.config_hash,
            "git_commit": metadata.git_commit,
            "sources": sorted(metadata.sources) if metadata.sources else [],
            "schema_id": metadata.schema_id,
            "schema_version": metadata.schema_version,
            "column_order_source": metadata.column_order_source,
            "na_policy": metadata.na_policy,
            "precision_policy": metadata.precision_policy,
        }

        if metadata.hash_policy_version:
            meta_dict["hash_policy_version"] = metadata.hash_policy_version

        config_snapshot = self._resolve_config_snapshot()
        if config_snapshot is not None:
            meta_dict["config_snapshot"] = config_snapshot

        if isinstance(dataset_path, dict):
            dataset_artifact: Any = {
                fmt: str(path) for fmt, path in dataset_path.items()
            }
        else:
            dataset_artifact = str(dataset_path)

        artifacts: dict[str, Any] = {
            "dataset": dataset_artifact,
            "quality_report": str(quality_path),
        }

        if additional_paths:
            serialized_additional: dict[str, Any] = {}
            for name, bundle in additional_paths.items():
                serialized_additional[name] = {
                    "datasets": {
                        fmt: str(path) for fmt, path in bundle.datasets.items()
                    },
                    "quality_report": str(bundle.quality_report),
                    "metadata": str(bundle.metadata),
                }
            artifacts["additional_datasets"] = serialized_additional

        if qc_artifacts:
            artifact_paths = {
                name: str(path) for name, path in qc_artifacts.items() if path is not None
            }
            if artifact_paths:
                artifacts["qc"] = artifact_paths

        # finalize artifacts
        meta_dict["artifacts"] = artifacts

        if qc_summary:
            meta_dict["qc_summary"] = qc_summary

        if qc_metrics:
            meta_dict["qc_metrics"] = qc_metrics

        if issues:
            meta_dict["validation_issues"] = issues

        if runtime_options:
            meta_dict["runtime_options"] = runtime_options

        lineage: dict[str, Any] | None = meta_dict.get("lineage")
        if not isinstance(lineage, dict):
            lineage = {"source_files": [], "transformations": []}
        else:
            lineage.setdefault("source_files", [])
            lineage.setdefault("transformations", [])
        meta_dict["lineage"] = lineage

        temp_dir_token = _ATOMIC_TEMP_DIR_NAME.set(f".tmp_run_{self.run_id}")

        def write_metadata() -> None:
            temp_path = _get_active_atomic_temp_path()
            with temp_path.open("w", encoding="utf-8") as handle:
                yaml.dump(
                    meta_dict,
                    handle,
                    default_flow_style=False,
                    sort_keys=True,
                )

        try:
            _atomic_write(path, write_metadata)
        finally:
            _ATOMIC_TEMP_DIR_NAME.reset(temp_dir_token)

    def _resolve_config_snapshot(self) -> dict[str, str] | None:
        """Return config snapshot metadata if a pipeline config is attached."""

        config = getattr(self, "pipeline_config", None)
        if config is None:
            return None

        source_path: Path | None = None

        candidate_path = getattr(config, "source_path", None)
        if candidate_path is not None:
            try:
                source_path = Path(candidate_path).resolve()
            except (TypeError, OSError):  # pragma: no cover - defensive guard
                source_path = None

        if source_path is None:
            return None

        try:
            digest = hashlib.sha256(source_path.read_bytes()).hexdigest()
        except OSError:
            return None

        relative: Path | str | None = None

        relative_accessor = getattr(config, "relative_source_path", None)
        if callable(relative_accessor):
            try:
                relative = relative_accessor()
            except TypeError:  # pragma: no cover - defensive guard
                relative = relative_accessor(source_path)
        elif hasattr(config, "source_path_relative"):
            relative = getattr(config, "source_path_relative")

        if isinstance(relative, Path):
            relative_path = relative
        elif isinstance(relative, str):
            relative_path = Path(relative)
        else:
            base = Path.cwd()
            try:
                relative_path = source_path.relative_to(base)
            except ValueError:
                try:
                    relative_path = Path(os.path.relpath(source_path, base))
                except OSError:
                    relative_path = source_path

        return {
            "path": relative_path.as_posix(),
            "sha256": f"sha256:{digest}",
        }

    def _write_json_atomic(self, path: Path, payload: Any) -> None:
        """Atomically write JSON payload to disk."""
        temp_dir_token = _ATOMIC_TEMP_DIR_NAME.set(f".tmp_run_{self.run_id}")

        def write_payload() -> None:
            temp_path = _get_active_atomic_temp_path()
            with temp_path.open("w", encoding="utf-8") as handle:
                if isinstance(payload, str):
                    handle.write(payload)
                else:
                    json.dump(payload, handle, indent=2, ensure_ascii=False, sort_keys=True)

        try:
            _atomic_write(path, write_payload)
        finally:
            _ATOMIC_TEMP_DIR_NAME.reset(temp_dir_token)

