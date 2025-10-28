"""UnifiedOutputWriter: deterministic data writing with quality metrics and atomic writes."""

import hashlib
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
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

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Создает QC отчет."""
        metrics = []

        for column in df.columns:
            null_count = df[column].isna().sum()
            null_fraction = null_count / len(df) if len(df) > 0 else 0
            unique_count = df[column].nunique()

            metrics.append(
                {
                    "column": column,
                    "null_count": null_count,
                    "null_fraction": null_fraction,
                    "unique_count": unique_count,
                    "dtype": str(df[column].dtype),
                }
            )

        return pd.DataFrame(metrics)


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
        quality_df = self.quality_generator.generate(df)
        self.atomic_writer.write(quality_df, quality_path)

        # Calculate checksums
        checksums = self._calculate_checksums(dataset_path, quality_path)

        # Write metadata if extended
        metadata_path = None
        if extended:
            metadata_path = output_path.parent / f"{base_name}_meta.yaml"
            self._write_metadata(metadata_path, metadata, checksums)

        return OutputArtifacts(
            dataset=dataset_path,
            quality_report=quality_path,
            metadata=metadata_path,
        )

    def _calculate_checksums(self, *paths: Path) -> dict[str, str]:
        """Вычисляет checksums для файлов."""
        checksums = {}
        for path in paths:
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
    ) -> None:
        """Записывает метаданные в YAML."""
        import yaml

        meta_dict = {
            "pipeline_version": metadata.pipeline_version,
            "source_system": metadata.source_system,
            "chembl_release": metadata.chembl_release,
            "extraction_timestamp": metadata.generated_at,
            "row_count": metadata.row_count,
            "file_checksums": checksums,
        }

        with path.open("w") as f:
            yaml.dump(meta_dict, f, default_flow_style=False, sort_keys=True)

