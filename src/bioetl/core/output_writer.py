"""UnifiedOutputWriter: deterministic data writing with quality metrics and atomic writes."""

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.core.logger import UnifiedLogger

logger = UnifiedLogger.get(__name__)


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
        temp_dir = path.parent / f".tmp_run_{self.run_id}"
        temp_dir.mkdir(parents=True, exist_ok=True)

        temp_path = temp_dir / f"{path.name}.tmp"

        try:
            self._write_to_file(data, temp_path, **kwargs)
            path.parent.mkdir(parents=True, exist_ok=True)
            os.replace(str(temp_path), str(path))
        except Exception:
            temp_path.unlink(missing_ok=True)
            raise
        finally:
            try:
                if temp_dir.exists() and not any(temp_dir.iterdir()):
                    temp_dir.rmdir()
                elif temp_dir.exists():
                    for temp_file in temp_dir.glob("*.tmp"):
                        temp_file.unlink(missing_ok=True)
            except OSError:
                pass

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

    def __init__(self, run_id: str):
        self.run_id = run_id
        self.atomic_writer = AtomicWriter(run_id)
        self.quality_generator = QualityReportGenerator()

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

        # Generate metadata if not provided
        if metadata is None:
            metadata = OutputMetadata.from_dataframe(
                df,
                run_id=self.run_id,
                column_order=list(df.columns),
            )
        elif metadata.run_id is None:
            metadata = OutputMetadata(
                pipeline_version=metadata.pipeline_version,
                source_system=metadata.source_system,
                chembl_release=metadata.chembl_release,
                generated_at=metadata.generated_at,
                row_count=metadata.row_count,
                column_count=metadata.column_count,
                column_order=metadata.column_order,
                checksums=metadata.checksums,
                run_id=self.run_id,
            )

        logger.info(
            "writing_dataset",
            path=str(dataset_path),
            rows=len(df),
            run_directory=str(run_directory),
        )
        self.atomic_writer.write(df, dataset_path)

        logger.info(
            "generating_quality_report",
            path=str(quality_path),
            rows=len(df.columns),
        )
        quality_df = self.quality_generator.generate(
            df,
            issues=issues,
            qc_metrics=qc_metrics,
        )
        self.atomic_writer.write(quality_df, quality_path)

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
            self.atomic_writer.write(qc_missing_mappings, missing_mappings_path)

        enrichment_metrics_path: Path | None = None
        if qc_enrichment_metrics is not None and not qc_enrichment_metrics.empty:
            enrichment_metrics_path = qc_dir / "qc_enrichment_metrics.csv"
            logger.info(
                "writing_qc_enrichment_metrics",
                path=str(enrichment_metrics_path),
                rows=len(qc_enrichment_metrics),
            )
            self.atomic_writer.write(qc_enrichment_metrics, enrichment_metrics_path)

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
                self.atomic_writer.write(table, table_path)
                additional_paths[name] = table_path

        checksums = self._calculate_checksums(
            dataset_path,
            quality_path,
            *(additional_paths.values()),
            missing_mappings_path,
            enrichment_metrics_path,
            qc_summary_path,
        )

        metadata_path = run_directory / "meta.yaml"
        qc_artifact_paths = {
            "qc_summary": qc_summary_path,
            "qc_missing_mappings": missing_mappings_path,
            "qc_enrichment_metrics": enrichment_metrics_path,
        }

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
            metadata=metadata_path,
            qc_summary=qc_summary_path,
            qc_missing_mappings=missing_mappings_path,
            qc_enrichment_metrics=enrichment_metrics_path,
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

        json_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_json_atomic(json_path, parsed_payload)

    def _calculate_checksums(self, *paths: Path) -> dict[str, str]:
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

        temp_dir = path.parent / f".tmp_run_{self.run_id}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp_path = temp_dir / f"{path.name}.tmp"

        try:
            with temp_path.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, ensure_ascii=False, sort_keys=True)
            path.parent.mkdir(parents=True, exist_ok=True)
            os.replace(str(temp_path), str(path))
        except Exception:
            temp_path.unlink(missing_ok=True)
            raise
        finally:
            try:
                if temp_dir.exists() and not any(temp_dir.iterdir()):
                    temp_dir.rmdir()
                elif temp_dir.exists():
                    for temp_file in temp_dir.glob("*.tmp"):
                        temp_file.unlink(missing_ok=True)
            except OSError:
                pass

