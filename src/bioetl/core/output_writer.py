"""UnifiedOutputWriter: deterministic data writing with quality metrics and atomic writes."""

import hashlib
import json
import os
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from bioetl.core.logger import UnifiedLogger

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
class OutputArtifacts:
    """Пути к стандартным выходным артефактам."""

    dataset: Path
    quality_report: Path
    run_directory: Path
    additional_datasets: dict[str, Path] = field(default_factory=dict)
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

    @classmethod
    def from_dataframe(
        cls,
        df: pd.DataFrame,
        pipeline_version: str = "1.0.0",
        source_system: str = "unified",
        chembl_release: str | None = None,
        column_order: list[str] | None = None,
        run_id: str | None = None,
    ) -> "OutputMetadata":
        """Создает метаданные из DataFrame."""
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
        )


class AtomicWriter:
    """Атомарная запись с защитой от corruption."""

    def __init__(self, run_id: str):
        self.run_id = run_id

    def write(self, data: pd.DataFrame, path: Path, **kwargs) -> None:
        """Записывает data в path атомарно через run-scoped temp directory."""
        temp_dir_token = _ATOMIC_TEMP_DIR_NAME.set(f".tmp_run_{self.run_id}")

        def write_payload() -> None:
            temp_path = _get_active_atomic_temp_path()
            self._write_to_file(data, temp_path, **kwargs)

        try:
            _atomic_write(path, write_payload)
        finally:
            _ATOMIC_TEMP_DIR_NAME.reset(temp_dir_token)

    def _write_to_file(self, data: pd.DataFrame, path: Path, **kwargs) -> None:
        """Записывает DataFrame в файл."""
        data.to_csv(path, index=False, **kwargs)


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

    def __init__(self, run_id: str, determinism: Any | None = None):
        self.run_id = run_id
        self.atomic_writer = AtomicWriter(run_id)
        self.quality_generator = QualityReportGenerator()
        self.determinism = determinism

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
        determinism: Any | None = None,
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
        dataset_path = output_path
        if dataset_path.suffix != ".csv":
            dataset_path = dataset_path.with_suffix(".csv")

        dataset_dir = dataset_path.parent
        dataset_dir.mkdir(parents=True, exist_ok=True)

        run_directory = dataset_dir
        if dataset_dir.name.lower() in {"datasets", "tables", "csv"}:
            run_directory = dataset_dir.parent

        qc_dir = run_directory / "qc"
        qc_dir.mkdir(parents=True, exist_ok=True)

        quality_path = qc_dir / f"{dataset_path.stem}_quality_report.csv"

        resolved_determinism = determinism or self.determinism

        float_format: str | None = None
        configured_order: list[str] = []
        if resolved_determinism is not None:
            precision = getattr(resolved_determinism, "float_precision", None)
            if isinstance(precision, int):
                float_format = f"%.{precision}f"

            configured_order = list(getattr(resolved_determinism, "column_order", []) or [])

        export_df = df
        if configured_order:
            export_df = df.copy()

            missing_columns = [
                column for column in configured_order if column not in export_df.columns
            ]
            for column in missing_columns:
                export_df[column] = pd.NA

            extra_columns = [
                column for column in export_df.columns if column not in configured_order
            ]
            export_df = export_df[configured_order + extra_columns]

        # Generate metadata if not provided
        if metadata is None:
            metadata = OutputMetadata.from_dataframe(
                export_df,
                run_id=self.run_id,
                column_order=list(export_df.columns),
            )
        else:
            metadata_kwargs: dict[str, Any] = {}
            if metadata.run_id is None:
                metadata_kwargs["run_id"] = self.run_id
            if configured_order:
                metadata_kwargs["column_order"] = list(export_df.columns)
                metadata_kwargs["column_count"] = len(export_df.columns)
            if metadata_kwargs:
                metadata = replace(metadata, **metadata_kwargs)

        logger.info(
            "writing_dataset",
            path=str(dataset_path),
            rows=len(export_df),
            run_directory=str(run_directory),
        )
        write_kwargs: dict[str, Any] = {}
        if float_format is not None:
            write_kwargs["float_format"] = float_format

        self.atomic_writer.write(export_df, dataset_path, **write_kwargs)

        logger.info(
            "generating_quality_report",
            path=str(quality_path),
            rows=len(export_df.columns),
        )
        quality_df = self.quality_generator.generate(
            export_df,
            issues=issues,
            qc_metrics=qc_metrics,
        )
        self.atomic_writer.write(quality_df, quality_path, **write_kwargs)

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
            self.atomic_writer.write(qc_missing_mappings, missing_mappings_path, **write_kwargs)

        enrichment_metrics_path: Path | None = None
        if qc_enrichment_metrics is not None and not qc_enrichment_metrics.empty:
            enrichment_metrics_path = qc_dir / "qc_enrichment_metrics.csv"
            logger.info(
                "writing_qc_enrichment_metrics",
                path=str(enrichment_metrics_path),
                rows=len(qc_enrichment_metrics),
            )
            self.atomic_writer.write(
                qc_enrichment_metrics, enrichment_metrics_path, **write_kwargs
            )

        additional_paths: dict[str, Path] = {}
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
                        if relative_path.suffix != ".csv":
                            relative_path = relative_path.with_suffix(".csv")
                        table_path = run_directory / relative_path
                else:
                    safe_name = name.replace(" ", "_").lower()
                    table_path = dataset_dir / f"{safe_name}.csv"

                logger.info(
                    "writing_additional_dataset",
                    name=name,
                    path=str(table_path),
                    rows=len(table),
                )
                self.atomic_writer.write(table, table_path, **write_kwargs)
                additional_paths[name] = table_path

        correlation_path: Path | None = None
        summary_statistics_path: Path | None = None
        dataset_metrics_path: Path | None = None

        if extended:
            correlation_df = self._build_correlation_report(export_df)
            if correlation_df is not None and not correlation_df.empty:
                correlation_path = qc_dir / f"{dataset_path.stem}_correlation_report.csv"
                logger.info(
                    "writing_correlation_report",
                    path=str(correlation_path),
                    rows=len(correlation_df),
                )
                self.atomic_writer.write(correlation_df, correlation_path, **write_kwargs)
            else:
                logger.info(
                    "skip_correlation_report",
                    reason="no_numeric_columns" if correlation_df is None else "empty_payload",
                )

            summary_statistics_df = self._build_summary_statistics(export_df)
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
                    summary_statistics_df, summary_statistics_path, **write_kwargs
                )

            dataset_metrics_df = self._build_dataset_metrics(export_df)
            if dataset_metrics_df is not None and not dataset_metrics_df.empty:
                dataset_metrics_path = qc_dir / f"{dataset_path.stem}_dataset_metrics.csv"
                logger.info(
                    "writing_dataset_metrics",
                    path=str(dataset_metrics_path),
                    rows=len(dataset_metrics_df),
                )
                self.atomic_writer.write(
                    dataset_metrics_df, dataset_metrics_path, **write_kwargs
                )

        checksum_targets: list[Path] = [
            dataset_path,
            quality_path,
            *additional_paths.values(),
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

        checksums = self._calculate_checksums(*checksum_targets)

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
        )

        return OutputArtifacts(
            dataset=dataset_path,
            quality_report=quality_path,
            run_directory=run_directory,
            additional_datasets=additional_paths,
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
        date_format: str = "iso",
    ) -> None:
        """Serialize ``df`` to JSON using the same atomic guarantees as CSV writes."""

        logger.info(
            "writing_dataframe_json",
            path=str(json_path),
            rows=len(df),
            orient=orient,
            date_format=date_format,
        )

        json_payload = df.to_json(
            orient=orient,
            force_ascii=False,
            date_format=date_format,
        )
        parsed_payload = json.loads(json_payload) if json_payload else []

        self._write_json_atomic(json_path, parsed_payload)

    def _calculate_checksums(self, *paths: Path | None) -> dict[str, str]:
        """Вычисляет checksums для файлов."""
        checksums = {}
        for path in paths:
            if path is None:
                continue
            if path.exists():
                with path.open("rb") as f:
                    content = f.read()
                    checksums[path.name] = hashlib.sha256(content).hexdigest()
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
        duplicate_rows = int(df.duplicated().sum()) if row_count else 0
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
        dataset_path: Path,
        quality_path: Path,
        additional_paths: dict[str, Path] | None = None,
        qc_summary: dict[str, Any] | None = None,
        qc_metrics: dict[str, Any] | None = None,
        issues: list[dict[str, Any]] | None = None,
        qc_artifacts: dict[str, Path | None] | None = None,
        runtime_options: dict[str, Any] | None = None,
    ) -> None:
        """Записывает метаданные в YAML."""
        import yaml

        meta_dict = {
            "run_id": metadata.run_id,
            "pipeline_version": metadata.pipeline_version,
            "source_system": metadata.source_system,
            "chembl_release": metadata.chembl_release,
            "extraction_timestamp": metadata.generated_at,
            "row_count": metadata.row_count,
            "column_count": metadata.column_count,
            "column_order": metadata.column_order,
            "file_checksums": checksums,
            "artifacts": {
                "dataset": str(dataset_path),
                "quality_report": str(quality_path),
            },
        }

        if additional_paths:
            meta_dict.setdefault("artifacts", {})["additional_datasets"] = {
                name: str(path) for name, path in additional_paths.items()
            }

        if qc_artifacts:
            artifact_paths = {
                name: str(path) for name, path in qc_artifacts.items() if path is not None
            }
            if artifact_paths:
                meta_dict.setdefault("artifacts", {})["qc"] = artifact_paths

        if qc_summary:
            meta_dict["qc_summary"] = qc_summary

        if qc_metrics:
            meta_dict["qc_metrics"] = qc_metrics

        if issues:
            meta_dict["validation_issues"] = issues

        if runtime_options:
            meta_dict["runtime_options"] = runtime_options

        with path.open("w") as f:
            yaml.dump(meta_dict, f, default_flow_style=False, sort_keys=True)

    def _write_json_atomic(self, path: Path, payload: Any) -> None:
        """Atomically write JSON payload to disk."""
        temp_dir_token = _ATOMIC_TEMP_DIR_NAME.set(f".tmp_run_{self.run_id}")

        def write_payload() -> None:
            temp_path = _get_active_atomic_temp_path()
            with temp_path.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, ensure_ascii=False, sort_keys=True)

        try:
            _atomic_write(path, write_payload)
        finally:
            _ATOMIC_TEMP_DIR_NAME.reset(temp_dir_token)

