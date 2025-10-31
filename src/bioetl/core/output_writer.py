"""UnifiedOutputWriter: deterministic data writing with quality metrics and atomic writes."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any
import typing as _typing

import pandas as pd
from pandas import DataFrame

from bioetl.config.models import DeterminismConfig
from bioetl.config.paths import get_configs_root
from bioetl.core.logger import UnifiedLogger
from bioetl.core.hashing import (
    generate_hash_business_key as _generate_hash_business_key,
    generate_hash_row as _generate_hash_row,
)
from bioetl.pandera_pandas import DataFrameModel
from bioetl.utils.dataframe import resolve_schema_column_order

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


class OutputWriterStage(str, Enum):
    """Named stages used to contextualise logging emitted by the writer."""

    VALIDATION = "validation"
    FORMAT = "format"
    QUALITY = "quality"
    METADATA = "metadata"
    ATOMIC_WRITE = "atomic_write"


def hash_row(payload: dict[str, Any]) -> str:
    """Return the canonical ``hash_row`` digest for ``payload``."""

    return _generate_hash_row(payload)


def hash_business_key(value: Any) -> str:
    """Return the canonical ``hash_business_key`` digest for ``value``."""

    return _generate_hash_business_key(value)


@dataclass(frozen=True)
class OutputArtifacts:
    """Пути к стандартным выходным артефактам."""

    dataset: Path
    quality_report: Path
    run_directory: Path
    additional_datasets: dict[str, Path | dict[str, Path]] = field(default_factory=dict)
    correlation_report: Path | None = None
    metadata: Path | None = None
    manifest: Path | None = None
    qc_summary: Path | None = None
    qc_missing_mappings: Path | None = None
    qc_enrichment_metrics: Path | None = None
    qc_summary_statistics: Path | None = None
    qc_dataset_metrics: Path | None = None
    debug_dataset: Path | None = None
    metadata_model: OutputMetadata | None = None
    hash_summary: dict[str, Any] | None = None


_SUPPORTED_FORMATS = {"csv", "parquet"}


__all__ = [
    "normalise_output_format",
    "extension_for_format",
    "OutputArtifacts",
    "AdditionalTableSpec",
    "OutputMetadata",
    "QualityReportGenerator",
    "UnifiedOutputWriter",
    "hash_row",
    "hash_business_key",
]


def normalise_output_format(
    value: str | None,
    *,
    default: str | None = "csv",
) -> str | None:
    """Normalise a format string to lower-case and validate support."""

    if value is None:
        return default

    resolved = value.strip().lower()
    if resolved not in _SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported output format: {value}")
    return resolved


def extension_for_format(format_name: str) -> str:
    """Return the canonical file extension for the given format."""

    resolved = normalise_output_format(format_name)
    if resolved is None:
        raise ValueError("Format name cannot be empty")
    return ".parquet" if resolved == "parquet" else ".csv"


@dataclass(frozen=True)
class AdditionalTableSpec:
    """Описание дополнительной таблицы для экспорта."""

    dataframe: DataFrame
    relative_path: Path | None = None
    formats: tuple[str, ...] = ("csv",)


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
    hash_summary: dict[str, Any] | None = None
    quantitative_metrics: dict[str, Any] = field(default_factory=dict)
    config_version: str | None = None
    stage_durations_ms: dict[str, float] | None = None
    sort_keys: dict[str, Any] | None = None
    pii_secrets_policy: dict[str, Any] | None = None

    @staticmethod
    def default_pii_policy() -> dict[str, Any]:
        """Return the standard data-protection policy snapshot for metadata."""

        return {
            "pii_expected": False,
            "pii_controls": "Derived from public ChEMBL records; personal identifiers are removed upstream.",
            "secret_management": "API credentials are provided via environment variables and redacted in structured logs.",
        }

    @classmethod
    def from_dataframe(
        cls,
        df: DataFrame,
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
        config_version: str | None = None,
        quantitative_metrics: Mapping[str, Any] | None = None,
        pii_secrets_policy: Mapping[str, Any] | None = None,
    ) -> OutputMetadata:
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

            registration = SchemaRegistry.find_registration(
                _typing.cast(DataFrameModel, schema_cls)
            )
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

        base_metrics: dict[str, Any] = {
            "row_count": len(df),
            "column_count": len(df.columns),
        }
        if quantitative_metrics:
            base_metrics.update(dict(quantitative_metrics))

        resolved_pii_policy = (
            dict(pii_secrets_policy)
            if pii_secrets_policy
            else cls.default_pii_policy()
        )

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
            quantitative_metrics=base_metrics,
            config_version=config_version,
            pii_secrets_policy=resolved_pii_policy,
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

    def _logger(self):
        return logger.bind(stage=OutputWriterStage.ATOMIC_WRITE.value, run_id=self.run_id)

    def write(
        self,
        data: DataFrame,
        path: Path,
        *,
        format: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Записывает data в path атомарно через run-scoped temp directory."""

        temp_dir_token = _ATOMIC_TEMP_DIR_NAME.set(f".tmp_run_{self.run_id}")

        resolved_format = normalise_output_format(format, default=None)
        if resolved_format is None:
            suffix = path.suffix.lower()
            resolved_format = (
                normalise_output_format(suffix.lstrip(".")) if suffix else "csv"
            )

        resolved_float_format = _resolve_float_format(
            self.determinism, kwargs.pop("float_format", None)
        )
        resolved_date_format = _resolve_date_format(
            self.determinism, kwargs.pop("date_format", None)
        )

        assert resolved_format is not None

        def write_payload() -> None:
            temp_path = _get_active_atomic_temp_path()
            self._write_to_file(
                data,
                temp_path,
                format=resolved_format,
                float_format=resolved_float_format,
                date_format=resolved_date_format,
                **kwargs,
            )

        try:
            self._logger().info(
                "atomic_write_dataset",
                path=str(path),
                format=resolved_format,
            )
            _atomic_write(path, write_payload)
        finally:
            _ATOMIC_TEMP_DIR_NAME.reset(temp_dir_token)

    def _write_to_file(
        self,
        data: DataFrame,
        path: Path,
        *,
        format: str,
        float_format: str | None = None,
        date_format: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Записывает DataFrame в файл."""

        if format == "csv":
            data.to_csv(
                path,
                index=False,
                float_format=float_format,
                date_format=date_format,
                **kwargs,
            )
            return

        if format == "parquet":
            parquet_kwargs = {key: value for key, value in kwargs.items() if key not in {"float_format", "date_format"}}
            data.to_parquet(path, index=False, **parquet_kwargs)
            return

        raise ValueError(f"Unsupported output format: {format}")

    def write_json(
        self,
        payload: Any,
        path: Path,
        *,
        indent: int = 2,
        ensure_ascii: bool = False,
        sort_keys: bool = True,
    ) -> None:
        """Atomically serialise ``payload`` to JSON using the writer's temp dir."""

        temp_dir_token = _ATOMIC_TEMP_DIR_NAME.set(f".tmp_run_{self.run_id}")

        def write_payload() -> None:
            temp_path = _get_active_atomic_temp_path()
            with temp_path.open("w", encoding="utf-8") as handle:
                if isinstance(payload, str):
                    handle.write(payload)
                else:
                    json.dump(
                        payload,
                        handle,
                        indent=indent,
                        ensure_ascii=ensure_ascii,
                        sort_keys=sort_keys,
                    )

        try:
            self._logger().info("atomic_write_json", path=str(path))
            _atomic_write(path, write_payload)
        finally:
            _ATOMIC_TEMP_DIR_NAME.reset(temp_dir_token)


class QualityReportGenerator:
    """Генератор quality report."""

    def generate(
        self,
        df: DataFrame,
        issues: list[dict[str, Any]] | None = None,
        qc_metrics: dict[str, Any] | None = None,
    ) -> DataFrame:
        """Создает QC отчет."""
        rows: list[dict[str, Any]] = []
        row_count = len(df)
        null_counts = df.isna().sum()
        unique_counts = df.nunique(dropna=True)
        dtype_strings = df.dtypes.astype(str)

        for column in df.columns:
            null_count = int(null_counts[column])
            null_fraction = null_count / row_count if row_count > 0 else 0
            unique_count = int(unique_counts[column])

            rows.append(
                {
                    "metric": "column_profile",
                    "column": column,
                    "null_count": null_count,
                    "null_fraction": null_fraction,
                    "unique_count": unique_count,
                    "dtype": dtype_strings[column],
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

        result_df: DataFrame = DataFrame(rows)
        return result_df


class UnifiedOutputWriter:
    """Unified output writer with atomic writes and QC reports."""

    def __init__(
        self,
        run_id: str,
        determinism: DeterminismConfig | None = None,
        pipeline_config: PipelineConfig | None = None,
    ):
        self.run_id = run_id
        self.determinism = determinism or DeterminismConfig()
        self.atomic_writer = AtomicWriter(run_id, determinism=self.determinism)
        self.quality_generator = QualityReportGenerator()
        self.pipeline_config: PipelineConfig | None = pipeline_config

    def _stage_logger(self, stage: OutputWriterStage):
        """Return a logger bound with the output writer stage."""

        return logger.bind(stage=stage.value, run_id=self.run_id)

    def _resolve_schema_order_from_metadata(
        self, metadata: OutputMetadata | None
    ) -> list[str]:
        """Resolve schema order using the ``schema_id`` and ``schema_version`` fields."""

        if metadata is None or not metadata.schema_id:
            return []

        try:
            from bioetl.schemas.registry import SchemaRegistry  # noqa: PLC0415
        except Exception:  # pragma: no cover - defensive guard
            return []

        registration = SchemaRegistry.find_registration_by_schema_id(
            metadata.schema_id,
            version=metadata.schema_version,
        )
        if registration is None:
            return []

        try:
            return resolve_schema_column_order(registration.schema)
        except Exception:  # pragma: no cover - defensive guard
            return []

    def _resolve_schema_order_from_config(self) -> list[str]:
        """Resolve schema order using the attached pipeline configuration."""

        if self.pipeline_config is None:
            return []

        pipeline_section = getattr(self.pipeline_config, "pipeline", None)
        if pipeline_section is None:
            return []

        entity = getattr(pipeline_section, "entity", None)
        if not entity:
            return []

        version = getattr(pipeline_section, "version", "latest")

        try:
            from bioetl.schemas.registry import SchemaRegistry  # noqa: PLC0415
        except Exception:  # pragma: no cover - defensive guard
            return []

        registration = SchemaRegistry.get_metadata(entity, version)
        if registration is None:
            return []

        try:
            return resolve_schema_column_order(registration.schema)
        except Exception:  # pragma: no cover - defensive guard
            return []

    def _resolve_target_order(
        self,
        metadata: OutputMetadata | None,
    ) -> tuple[list[str], bool]:
        """Return the desired column order and whether strict enforcement is required."""

        if metadata is not None and metadata.column_order:
            order = [column for column in metadata.column_order if column]
            strict = metadata.column_order_source in {"schema_registry", "schema"}
            return order, strict

        configured_order = [column for column in self.determinism.column_order if column]
        if configured_order:
            return configured_order, False

        schema_order = self._resolve_schema_order_from_metadata(metadata)
        if schema_order:
            return schema_order, True

        schema_order = self._resolve_schema_order_from_config()
        if schema_order:
            return schema_order, True

        return [], False

    def _apply_column_order(
        self,
        df: DataFrame,
        metadata: OutputMetadata | None = None,
    ) -> DataFrame:
        """Return a dataframe matching the configured deterministic column order."""

        order, strict = self._resolve_target_order(metadata)
        stage_logger = self._stage_logger(OutputWriterStage.VALIDATION)

        if not order:
            return df

        missing_columns = [column for column in order if column not in df.columns]
        extra_columns = [column for column in df.columns if column not in order]

        if (missing_columns or extra_columns) and strict:
            stage_logger.error(
                "column_order_mismatch",
                expected=order,
                missing=missing_columns,
                unexpected=extra_columns,
            )
            raise ValueError(
                "columns do not match Schema Registry order: "
                f"missing {missing_columns}, unexpected {extra_columns}"
            )

        if not missing_columns and not extra_columns and list(df.columns) == order:
            return df

        ordered = df.copy()
        for column in missing_columns:
            ordered[column] = pd.NA

        if missing_columns:
            stage_logger.warning(
                "column_order_padding",
                added=missing_columns,
                source="schema" if strict else "determinism",
            )

        remaining_columns = [column for column in ordered.columns if column not in order]
        new_order = order + remaining_columns
        ordered_view: DataFrame = ordered[new_order]
        return ordered_view

    def _resolve_dataset_location(self, output_path: Path) -> tuple[Path, str]:
        """Return the concrete dataset path and serialisation format."""

        suffix = output_path.suffix.lower()
        if suffix in {".csv", ".parquet"}:
            return output_path, suffix.lstrip(".")

        return output_path.with_suffix(".csv"), "csv"

    def write(
        self,
        df: DataFrame,
        output_path: Path,
        metadata: OutputMetadata | None = None,
        extended: bool = False,
        issues: list[dict[str, Any]] | None = None,
        qc_metrics: dict[str, Any] | None = None,
        qc_summary: dict[str, Any] | None = None,
        qc_missing_mappings: DataFrame | None = None,
        qc_enrichment_metrics: DataFrame | None = None,
        additional_tables: dict[str, AdditionalTableSpec] | None = None,
        runtime_options: dict[str, Any] | None = None,
        debug_dataset: Path | None = None,
        *,
        apply_column_order: bool = True,
        stage_durations_ms: Mapping[str, float] | None = None,
        sort_definition: Mapping[str, Any] | None = None,
        pii_secrets_policy: Mapping[str, Any] | None = None,
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
        load_stage_start = perf_counter()

        dataset_df: DataFrame = df.copy()
        if apply_column_order:
            dataset_df = self._apply_column_order(dataset_df, metadata)
        column_order = list(dataset_df.columns)
        dataset_path, dataset_format = self._resolve_dataset_location(output_path)

        float_format: str | None = None
        if dataset_format == "csv":
            float_precision = self.determinism.float_precision
            float_format = f"%.{float_precision}f"

        dataset_dir = dataset_path.parent
        dataset_dir.mkdir(parents=True, exist_ok=True)

        run_directory = dataset_dir
        if dataset_dir.name.lower() in {"datasets", "tables", "csv"}:
            run_directory = dataset_dir.parent

        qc_dir = run_directory / "qc"
        qc_dir.mkdir(parents=True, exist_ok=True)

        quality_path = qc_dir / f"{dataset_path.stem}_quality_report.csv"

        # Generate metadata if not provided
        if metadata is None:
            metadata = OutputMetadata.from_dataframe(
                dataset_df,
                run_id=self.run_id,
                column_order=column_order,
                hash_policy_version=getattr(self.determinism, "hash_policy_version", None),
            )
        else:
            initial_metadata_updates: dict[str, Any] = {}
            if metadata.run_id is None:
                initial_metadata_updates["run_id"] = self.run_id
            if metadata.column_order != column_order:
                initial_metadata_updates["column_order"] = column_order
            if metadata.column_count != len(column_order):
                initial_metadata_updates["column_count"] = len(column_order)
            if metadata.row_count != len(dataset_df):
                initial_metadata_updates["row_count"] = len(dataset_df)

            if initial_metadata_updates:
                metadata = replace(metadata, **initial_metadata_updates)

        metadata_defaults: dict[str, Any] = {}
        if metadata.na_policy is None:
            metadata_defaults["na_policy"] = "allow"
        if metadata.precision_policy is None and float_format is not None:
            metadata_defaults["precision_policy"] = float_format
        if metadata.sort_keys is None and sort_definition is not None:
            metadata_defaults["sort_keys"] = dict(sort_definition)
        if metadata.pii_secrets_policy is None and pii_secrets_policy is not None:
            metadata_defaults["pii_secrets_policy"] = dict(pii_secrets_policy)
        if metadata.stage_durations_ms is None and stage_durations_ms is not None:
            metadata_defaults["stage_durations_ms"] = {
                str(name): float(value) for name, value in stage_durations_ms.items()
            }

        if metadata_defaults:
            metadata = replace(metadata, **metadata_defaults)

        self._stage_logger(OutputWriterStage.FORMAT).info(
            "writing_dataset",
            path=str(dataset_path),
            rows=len(dataset_df),
            run_directory=str(run_directory),
        )
        write_kwargs: dict[str, Any] = {}
        if float_format is not None:
            write_kwargs["float_format"] = float_format
        self.atomic_writer.write(
            dataset_df,
            dataset_path,
            format=dataset_format,
            **write_kwargs,
        )

        self._stage_logger(OutputWriterStage.QUALITY).info(
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

        hash_summary = self._compute_hash_summary(dataset_df)
        if hash_summary:
            metadata = replace(metadata, hash_summary=hash_summary)

        qc_summary_path: Path | None = None
        if qc_summary:
            qc_summary_path = qc_dir / "qc_summary.json"
            self._stage_logger(OutputWriterStage.QUALITY).info(
                "writing_qc_summary", path=str(qc_summary_path)
            )
            self._write_json_atomic(qc_summary_path, qc_summary)

        missing_mappings_path: Path | None = None
        if qc_missing_mappings is not None and not qc_missing_mappings.empty:
            missing_mappings_path = qc_dir / "qc_missing_mappings.csv"
            self._stage_logger(OutputWriterStage.QUALITY).info(
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
            self._stage_logger(OutputWriterStage.QUALITY).info(
                "writing_qc_enrichment_metrics",
                path=str(enrichment_metrics_path),
                rows=len(qc_enrichment_metrics),
            )
            self.atomic_writer.write(
                qc_enrichment_metrics,
                enrichment_metrics_path,
                float_format=float_format,
            )

        additional_paths: dict[str, Path | dict[str, Path]] = {}
        if additional_tables:
            for name, table_spec in additional_tables.items():
                if table_spec is None:
                    continue

                table = table_spec.dataframe
                if table is None or table.empty:
                    continue

                table_relative_path = table_spec.relative_path
                if table_relative_path is not None:
                    relative_path = Path(table_relative_path)
                    if relative_path.is_absolute():
                        table_path = relative_path
                    else:
                        table_path = run_directory / relative_path
                else:
                    safe_name = name.replace(" ", "_").lower()
                    table_path = dataset_dir / safe_name

                resolved_formats = table_spec.formats or ("csv",)
                normalised_formats = []
                for fmt in resolved_formats:
                    normalised = normalise_output_format(fmt)
                    if normalised is None:
                        continue
                    if normalised not in normalised_formats:
                        normalised_formats.append(normalised)

                table_artifacts: dict[str, Path] = {}
                for fmt in normalised_formats:
                    extension = extension_for_format(fmt)
                    target_path = table_path
                    if target_path.suffix:
                        target_path = target_path.with_suffix(extension)
                    else:
                        target_path = target_path.with_suffix(extension)

                    target_path.parent.mkdir(parents=True, exist_ok=True)

                    self._stage_logger(OutputWriterStage.FORMAT).info(
                        "writing_additional_dataset",
                        name=name,
                        path=str(target_path),
                        rows=len(table),
                        format=fmt,
                    )

                    write_args: dict[str, Any] = {}
                    if fmt == "csv" and float_format is not None:
                        write_args["float_format"] = float_format

                    self.atomic_writer.write(
                        table,
                        target_path,
                        format=fmt,
                        **write_args,
                    )
                    table_artifacts[fmt] = target_path

                if not table_artifacts:
                    continue

                if len(table_artifacts) == 1:
                    additional_paths[name] = next(iter(table_artifacts.values()))
                else:
                    additional_paths[name] = table_artifacts

        correlation_path: Path | None = None
        summary_statistics_path: Path | None = None
        dataset_metrics_path: Path | None = None
        dataset_metrics_payload: dict[str, Any] | None = None

        if extended:
            correlation_df = self._build_correlation_report(dataset_df)
            if correlation_df is not None and not correlation_df.empty:
                correlation_path = qc_dir / f"{dataset_path.stem}_correlation_report.csv"
                self._stage_logger(OutputWriterStage.QUALITY).info(
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
                self._stage_logger(OutputWriterStage.QUALITY).info(
                    "skip_correlation_report",
                    reason="no_numeric_columns" if correlation_df is None else "empty_payload",
                )

            summary_statistics_df = self._build_summary_statistics(dataset_df)
            if summary_statistics_df is not None and not summary_statistics_df.empty:
                summary_statistics_path = (
                    qc_dir / f"{dataset_path.stem}_summary_statistics.csv"
                )
                self._stage_logger(OutputWriterStage.QUALITY).info(
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
                self._stage_logger(OutputWriterStage.QUALITY).info(
                    "writing_dataset_metrics",
                    path=str(dataset_metrics_path),
                    rows=len(dataset_metrics_df),
                )
                self.atomic_writer.write(
                    dataset_metrics_df,
                    dataset_metrics_path,
                    float_format=float_format,
                )
                dataset_metrics_payload = {
                    str(row["metric"]): row["value"]
                    for row in dataset_metrics_df.to_dict("records")
                    if isinstance(row, dict) and "metric" in row and "value" in row
                }

        checksum_targets: list[Path] = [
            dataset_path,
            quality_path,
        ]

        for value in additional_paths.values():
            if isinstance(value, dict):
                checksum_targets.extend(value.values())
            elif isinstance(value, Path):
                checksum_targets.append(value)

        optional_targets: tuple[Path | None, ...] = (
            missing_mappings_path,
            enrichment_metrics_path,
            qc_summary_path,
            correlation_path,
            summary_statistics_path,
            dataset_metrics_path,
        )
        checksum_targets.extend(path for path in optional_targets if path is not None)

        if metadata.pii_secrets_policy is None:
            metadata = replace(
                metadata, pii_secrets_policy=OutputMetadata.default_pii_policy()
            )

        quantitative_metrics = dict(metadata.quantitative_metrics)
        if dataset_metrics_payload:
            quantitative_metrics.update(dataset_metrics_payload)
        metadata = replace(metadata, quantitative_metrics=quantitative_metrics)

        checksums = self._calculate_checksums(*checksum_targets)
        stage_duration_payload = dict(metadata.stage_durations_ms or {})
        load_duration_ms = (perf_counter() - load_stage_start) * 1000.0
        stage_duration_payload["load"] = load_duration_ms

        final_metadata_updates: dict[str, Any] = {
            "checksums": checksums,
            "stage_durations_ms": stage_duration_payload,
        }

        if metadata.sort_keys is None and sort_definition is not None:
            final_metadata_updates["sort_keys"] = dict(sort_definition)

        if metadata.pii_secrets_policy is None and pii_secrets_policy is not None:
            final_metadata_updates["pii_secrets_policy"] = dict(pii_secrets_policy)

        metadata = replace(metadata, **final_metadata_updates)

        metadata_filename = f"{dataset_path.stem}_meta.yaml"
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

        self._stage_logger(OutputWriterStage.METADATA).info(
            "writing_metadata",
            path=str(metadata_path),
            hash_summary_present=bool(hash_summary),
        )
        self._write_metadata(
            metadata_path,
            metadata,
            checksums,
            dataset_path=dataset_path,
            quality_path=quality_path,
            additional_paths=additional_paths,
            qc_summary=qc_summary,
            qc_metrics=qc_metrics,
            issues=issues,
            qc_artifacts=qc_artifact_paths,
            runtime_options=runtime_options,
            hash_summary=hash_summary,
        )

        manifest_path = run_directory / "run_manifest.json"
        manifest_artifacts: dict[str, Any] = {
            "dataset": str(dataset_path),
            "quality_report": str(quality_path),
            "metadata": str(metadata_path),
        }

        if additional_paths:
            additional_listing: dict[str, Any] = {}
            for name, value in additional_paths.items():
                if isinstance(value, dict):
                    additional_listing[name] = {fmt: str(path) for fmt, path in value.items()}
                else:
                    additional_listing[name] = str(value)
            if additional_listing:
                manifest_artifacts["additional_datasets"] = additional_listing

        manifest_qc_artifacts = {
            name: str(path)
            for name, path in qc_artifact_paths.items()
            if path is not None
        }
        if manifest_qc_artifacts:
            manifest_artifacts["qc"] = manifest_qc_artifacts

        if debug_dataset is not None:
            manifest_artifacts["debug_dataset"] = str(debug_dataset)

        manifest_payload = {
            "run_id": metadata.run_id or self.run_id,
            "artifacts": manifest_artifacts,
            "checksums": checksums,
            "schema": {
                "id": metadata.schema_id,
                "version": metadata.schema_version,
            },
        }

        if hash_summary:
            manifest_payload["hash_summary"] = hash_summary

        self._stage_logger(OutputWriterStage.METADATA).info(
            "writing_manifest",
            path=str(manifest_path),
        )
        self.atomic_writer.write_json(manifest_payload, manifest_path)

        return OutputArtifacts(
            dataset=dataset_path,
            quality_report=quality_path,
            run_directory=run_directory,
            additional_datasets=additional_paths,
            correlation_report=correlation_path,
            metadata=metadata_path,
            manifest=manifest_path,
            qc_summary=qc_summary_path,
            qc_missing_mappings=missing_mappings_path,
            qc_enrichment_metrics=enrichment_metrics_path,
            qc_summary_statistics=summary_statistics_path,
            qc_dataset_metrics=dataset_metrics_path,
            debug_dataset=debug_dataset,
            metadata_model=metadata,
            hash_summary=hash_summary,
        )

    def write_dataframe_json(
        self,
        df: DataFrame,
        json_path: Path,
        *,
        orient: str = "records",
        date_format: str | None = None,
    ) -> None:
        """Serialize ``df`` to JSON using the same atomic guarantees as CSV writes."""

        self._stage_logger(OutputWriterStage.FORMAT).info(
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

    def _compute_hash_summary(self, df: DataFrame) -> dict[str, Any] | None:
        """Return a digest summary for hash-related columns if present."""

        summary: dict[str, Any] = {}
        for column in ("hash_row", "hash_business_key"):
            if column not in df.columns:
                continue
            column_summary = self._hash_series_summary(df[column])
            summary[column] = column_summary

        return summary or None

    def _hash_series_summary(self, series: pd.Series) -> dict[str, Any]:
        """Aggregate ``series`` into deterministic hash summary statistics."""

        values: list[str] = []
        null_count = 0
        for value in series:
            if pd.isna(value):
                null_count += 1
                continue
            values.append(str(value))

        digest = hashlib.sha256()
        for value in values:
            digest.update(value.encode("utf-8"))
            digest.update(b"\n")

        unique_count = len(set(values))

        return {
            "count": len(values),
            "unique": unique_count,
            "null_count": null_count,
            "sha256": digest.hexdigest(),
        }

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

    def _build_correlation_report(self, df: DataFrame) -> DataFrame | None:
        """Prepare a tidy correlation report for numeric columns."""

        if df.empty:
            return None

        numeric_df: DataFrame = df.select_dtypes(include="number")
        if numeric_df.empty:
            return None

        correlation: DataFrame = numeric_df.corr(numeric_only=True)
        if correlation.empty:
            return None

        correlation = correlation.round(6)
        tidy_any = (
            correlation.reset_index()
            .rename(columns={"index": "feature_x"})
            .melt(id_vars="feature_x", var_name="feature_y", value_name="correlation")
        )
        tidy_df: DataFrame = tidy_any.dropna(subset=["correlation"])
        tidy_df = tidy_df.sort_values(["feature_x", "feature_y"]).reset_index(drop=True)
        return tidy_df

    def _build_summary_statistics(self, df: DataFrame) -> DataFrame | None:
        """Build descriptive statistics for all columns."""

        if df.empty:
            return None

        try:
            summary = df.describe(include="all")
        except (TypeError, ValueError):
            return None

        if summary.empty:
            return None

        summary_df: DataFrame = summary.transpose().reset_index().rename(
            columns={"index": "column"}
        )
        summary_df = summary_df.convert_dtypes()
        return summary_df

    def _build_dataset_metrics(self, df: DataFrame) -> DataFrame | None:
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

        metrics_df: DataFrame = DataFrame(metrics)
        metrics_df = metrics_df.convert_dtypes()
        return metrics_df

    def _write_metadata(
        self,
        path: Path,
        metadata: OutputMetadata,
        checksums: dict[str, str],
        *,
        dataset_path: Path,
        quality_path: Path,
        additional_paths: dict[str, Path | dict[str, Path]] | None = None,
        qc_summary: dict[str, Any] | None = None,
        qc_metrics: dict[str, Any] | None = None,
        issues: list[dict[str, Any]] | None = None,
        qc_artifacts: dict[str, Path | None] | None = None,
        runtime_options: dict[str, Any] | None = None,
        hash_summary: dict[str, Any] | None = None,
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
            "checksum_algorithm": "sha256",
            "config_hash": metadata.config_hash,
            "git_commit": metadata.git_commit,
            "sources": sorted(metadata.sources) if metadata.sources else [],
            "schema_id": metadata.schema_id,
            "schema_version": metadata.schema_version,
            "column_order_source": metadata.column_order_source,
            "na_policy": metadata.na_policy,
            "precision_policy": metadata.precision_policy,
            "quantitative_metrics": metadata.quantitative_metrics,
            "pii_secrets_policy": metadata.pii_secrets_policy,
        }

        if metadata.config_version is not None:
            meta_dict["config_version"] = metadata.config_version

        stage_durations_ms = {
            str(name): float(value) for name, value in (metadata.stage_durations_ms or {}).items()
        }
        meta_dict["stage_durations_ms"] = stage_durations_ms

        meta_dict["sort_keys"] = dict(metadata.sort_keys or {})

        pii_policy = metadata.pii_secrets_policy or {}
        meta_dict["pii_secrets_policy"] = pii_policy

        if metadata.hash_policy_version:
            meta_dict["hash_policy_version"] = metadata.hash_policy_version

        config_snapshot = self._resolve_config_snapshot()
        if config_snapshot is not None:
            meta_dict["config_snapshot"] = config_snapshot

        artifacts: dict[str, Any] = {
            "dataset": str(dataset_path),
            "quality_report": str(quality_path),
        }

        if additional_paths:
            formatted_additional: dict[str, Any] = {}
            for name, path_value in additional_paths.items():
                if isinstance(path_value, dict):
                    formatted_additional[name] = {
                        fmt: str(path) for fmt, path in path_value.items()
                    }
                else:
                    formatted_additional[name] = str(path_value)
            artifacts["additional_datasets"] = formatted_additional

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

        if hash_summary:
            meta_dict["hash_summary"] = hash_summary

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

        relative_path: Path | None = None

        try:
            configs_root = get_configs_root()
        except (FileNotFoundError, RuntimeError):  # pragma: no cover - defensive guard
            configs_root = None

        if configs_root is not None:
            try:
                relative_path = Path("configs") / source_path.relative_to(configs_root)
            except ValueError:
                relative_path = None

        if relative_path is None:
            relative_accessor = getattr(config, "relative_source_path", None)
            relative: Path | str | None
            if callable(relative_accessor):
                try:
                    relative = relative_accessor()
                except TypeError:  # pragma: no cover - defensive guard
                    relative = relative_accessor(source_path)
            elif hasattr(config, "source_path_relative"):
                relative = config.source_path_relative
            else:
                relative = None

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
        self.atomic_writer.write_json(payload, path)

