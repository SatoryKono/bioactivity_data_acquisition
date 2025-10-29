"""UnifiedOutputWriter: deterministic data writing with quality metrics and atomic writes."""

import hashlib
import json
import os
from dataclasses import dataclass
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
    correlation_report: Path | None = None
    metadata: Path | None = None
    manifest: Path | None = None
    qc_summary: Path | None = None
    qc_missing_mappings: Path | None = None
    qc_enrichment_metrics: Path | None = None


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

    @classmethod
    def from_dataframe(
        cls,
        df: pd.DataFrame,
        pipeline_version: str = "1.0.0",
        source_system: str = "unified",
        chembl_release: str | None = None,
        column_order: list[str] | None = None,
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
        except Exception as e:
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
        # Generate metadata if not provided
        if metadata is None:
            metadata = OutputMetadata.from_dataframe(df)

        # Create base paths
        # Use the path as-is if it already contains a date
        # Otherwise add current date tag
        base_name = output_path.stem
        if not base_name.endswith(datetime.now(timezone.utc).strftime("%Y%m%d")):
            date_tag = datetime.now(timezone.utc).strftime("%Y%m%d")
            base_name = f"{base_name}_{date_tag}"

        dataset_path = output_path.parent / f"{base_name}.csv"
        quality_path = output_path.parent / f"{base_name}_quality_report.csv"

        # Write main dataset
        logger.info("writing_dataset", path=dataset_path, rows=len(df))
        self.atomic_writer.write(df, dataset_path)

        # Generate and write quality report
        logger.info("generating_quality_report")
        quality_df = self.quality_generator.generate(
            df,
            issues=issues,
            qc_metrics=qc_metrics,
        )
        self.atomic_writer.write(quality_df, quality_path)

        qc_summary_path: Path | None = None
        if qc_summary:
            qc_summary_path = output_path.parent / f"{base_name}_qc_summary.json"
            logger.info("writing_qc_summary", path=str(qc_summary_path))
            self._write_json_atomic(qc_summary_path, qc_summary)

        missing_mappings_path: Path | None = None
        if qc_missing_mappings is not None and not qc_missing_mappings.empty:
            missing_mappings_path = (
                output_path.parent / f"{base_name}_qc_missing_mappings.csv"
            )
            logger.info(
                "writing_qc_missing_mappings",
                path=str(missing_mappings_path),
                rows=len(qc_missing_mappings),
            )
            self.atomic_writer.write(qc_missing_mappings, missing_mappings_path)

        enrichment_metrics_path: Path | None = None
        if qc_enrichment_metrics is not None and not qc_enrichment_metrics.empty:
            enrichment_metrics_path = (
                output_path.parent / f"{base_name}_qc_enrichment_metrics.csv"
            )
            logger.info(
                "writing_qc_enrichment_metrics",
                path=str(enrichment_metrics_path),
                rows=len(qc_enrichment_metrics),
            )
            self.atomic_writer.write(qc_enrichment_metrics, enrichment_metrics_path)

        # Calculate checksums
        checksums = self._calculate_checksums(
            dataset_path,
            quality_path,
            missing_mappings_path,
            enrichment_metrics_path,
            qc_summary_path,
        )

        # Write metadata if extended
        metadata_path = None
        if extended:
            metadata_path = output_path.parent / f"{base_name}_meta.yaml"
            qc_artifact_paths = {
                "qc_summary": qc_summary_path,
                "qc_missing_mappings": missing_mappings_path,
                "qc_enrichment_metrics": enrichment_metrics_path,
            }
            self._write_metadata(
                metadata_path,
                metadata,
                checksums,
                qc_summary=qc_summary,
                qc_metrics=qc_metrics,
                issues=issues,
                qc_artifacts=qc_artifact_paths,
            )

        return OutputArtifacts(
            dataset=dataset_path,
            quality_report=quality_path,
            metadata=metadata_path,
            qc_summary=qc_summary_path,
            qc_missing_mappings=missing_mappings_path,
            qc_enrichment_metrics=enrichment_metrics_path,
        )

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
        qc_summary: dict[str, Any] | None = None,
        qc_metrics: dict[str, Any] | None = None,
        issues: list[dict[str, Any]] | None = None,
        qc_artifacts: dict[str, Path | None] | None = None,
    ) -> None:
        """Записывает метаданные в YAML."""
        import yaml

        meta_dict = {
            "pipeline_version": metadata.pipeline_version,
            "source_system": metadata.source_system,
            "chembl_release": metadata.chembl_release,
            "extraction_timestamp": metadata.generated_at,
            "row_count": metadata.row_count,
            "column_count": metadata.column_count,
            "column_order": metadata.column_order,
            "file_checksums": checksums,
        }

        if qc_artifacts:
            artifact_paths = {
                name: str(path) for name, path in qc_artifacts.items() if path is not None
            }
            if artifact_paths:
                meta_dict["qc_artifacts"] = artifact_paths

        if qc_summary:
            meta_dict["qc_summary"] = qc_summary

        if qc_metrics:
            meta_dict["qc_metrics"] = qc_metrics

        if issues:
            meta_dict["validation_issues"] = issues

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

