"""
Модуль для построения стандартизированных метаданных ETL пайплайнов.

Предоставляет единую структуру метаданных для всех пайплайнов.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from library.config import Config


class SourceInfo(BaseModel):
    """Информация об источнике данных."""

    name: str = Field(..., description="Имя источника")
    version: str | None = Field(None, description="Версия источника")
    records_fetched: int = Field(0, description="Количество полученных записей")
    errors: int = Field(0, description="Количество ошибок")
    last_updated: str | None = Field(None, description="Время последнего обновления")


class ValidationInfo(BaseModel):
    """Информация о валидации."""

    schema_passed: bool = Field(True, description="Пройдена ли схема валидации")
    qc_passed: bool = Field(True, description="Пройден ли контроль качества")
    warnings: int = Field(0, description="Количество предупреждений")
    errors: int = Field(0, description="Количество ошибок")


class FileInfo(BaseModel):
    """Информация о файле."""

    filename: str = Field(..., description="Имя файла")
    size_bytes: int = Field(0, description="Размер файла в байтах")
    checksum: str | None = Field(None, description="SHA256 хеш файла")


class ExecutionInfo(BaseModel):
    """Информация о выполнении пайплайна."""

    run_id: str = Field(..., description="Уникальный ID запуска")
    started_at: str = Field(..., description="Время начала выполнения ISO 8601")
    completed_at: str = Field(..., description="Время завершения выполнения ISO 8601")
    duration_sec: float = Field(0.0, description="Длительность выполнения в секундах")
    memory_peak_mb: float | None = Field(None, description="Пиковое использование памяти в МБ")


class DataInfo(BaseModel):
    """Информация о данных."""

    row_count: int = Field(0, description="Общее количество строк")
    row_count_accepted: int = Field(0, description="Количество принятых строк")
    row_count_rejected: int = Field(0, description="Количество отклоненных строк")
    columns_count: int = Field(0, description="Количество колонок")


class PipelineMetadata(BaseModel):
    """Стандартизированные метаданные пайплайна."""

    # Информация о пайплайне
    pipeline: dict[str, Any] = Field(default_factory=dict, description="Информация о пайплайне")

    # Информация о выполнении
    execution: ExecutionInfo = Field(..., description="Информация о выполнении")

    # Информация о данных
    data: DataInfo = Field(..., description="Информация о данных")

    # Информация об источниках
    sources: list[SourceInfo] = Field(default_factory=list, description="Информация об источниках")

    # Информация о валидации
    validation: ValidationInfo = Field(..., description="Информация о валидации")

    # Информация о файлах
    files: dict[str, str | FileInfo] = Field(default_factory=dict, description="Информация о файлах")

    # Дополнительные метаданные
    metadata: dict[str, Any] = Field(default_factory=dict, description="Дополнительные метаданные")


class MetadataBuilder:
    """Построитель метаданных для ETL пайплайнов."""

    def __init__(self, config: dict[str, Any], entity_type: str):
        self.config = config
        self.entity_type = entity_type
        self.start_time = datetime.now()
        self.run_id = self._generate_run_id()
        self._files: dict[str, str] = {}
        self._checksums: dict[str, str] = {}

    def _generate_run_id(self) -> str:
        """Сгенерировать уникальный ID запуска."""
        timestamp = self.start_time.strftime("%Y%m%d_%H%M%S")
        return f"{self.entity_type}_{timestamp}_{hash(str(self.start_time)) % 100000000:08d}"

    def _calculate_file_checksum(self, file_path: Path) -> str:
        """Вычислить SHA256 хеш файла."""
        if not file_path.exists():
            return ""

        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return f"sha256:{sha256_hash.hexdigest()}"

    def _get_file_info(self, file_path: Path) -> FileInfo:
        """Получить информацию о файле."""
        if not file_path.exists():
            return FileInfo(filename=file_path.name, size_bytes=0)

        stat = file_path.stat()
        checksum = self._calculate_file_checksum(file_path)

        return FileInfo(filename=file_path.name, size_bytes=stat.st_size, checksum=checksum)

    def build_pipeline_info(self) -> dict[str, Any]:
        """Построить информацию о пайплайне."""
        pipeline_info = {
            "name": self.entity_type,
            "entity_type": self.entity_type,
        }

        # Получить версию из конфигурации
        pipeline_config = self.config.get("pipeline", {})
        if "version" in pipeline_config:
            pipeline_info["version"] = pipeline_config["version"]
        else:
            pipeline_info["version"] = "2.0.0"  # Default version

        # Получить source_system из конфигурации
        if "source_system" in pipeline_config:
            pipeline_info["source_system"] = pipeline_config["source_system"]
        else:
            pipeline_info["source_system"] = "chembl"

        # Получить chembl_release из конфигурации
        if "chembl_release" in pipeline_config:
            pipeline_info["chembl_release"] = pipeline_config["chembl_release"]

        return pipeline_info

    def build_execution_info(self, end_time: datetime | None = None) -> ExecutionInfo:
        """Построить информацию о выполнении."""
        if end_time is None:
            end_time = datetime.now()

        duration = (end_time - self.start_time).total_seconds()

        return ExecutionInfo(run_id=self.run_id, started_at=self.start_time.isoformat(), completed_at=end_time.isoformat(), duration_sec=duration)

    def build_data_info(self, df: Any, accepted_df: Any | None = None, rejected_df: Any | None = None) -> DataInfo:
        """Построить информацию о данных."""
        row_count = len(df) if hasattr(df, "__len__") else 0
        row_count_accepted = len(accepted_df) if accepted_df is not None and hasattr(accepted_df, "__len__") else row_count
        row_count_rejected = len(rejected_df) if rejected_df is not None and hasattr(rejected_df, "__len__") else 0
        columns_count = len(df.columns) if hasattr(df, "columns") else 0

        return DataInfo(row_count=row_count, row_count_accepted=row_count_accepted, row_count_rejected=row_count_rejected, columns_count=columns_count)

    def build_sources_info(self, extraction_results: dict[str, Any] | None = None) -> list[SourceInfo]:
        """Построить информацию об источниках."""
        sources = []

        if extraction_results:
            for source_name, source_data in extraction_results.items():
                if isinstance(source_data, dict):
                    records_fetched = source_data.get("records_fetched", 0)
                    errors = source_data.get("errors", 0)
                    version = source_data.get("version")
                    last_updated = source_data.get("last_updated")
                else:
                    records_fetched = len(source_data) if hasattr(source_data, "__len__") else 0
                    errors = 0
                    version = None
                    last_updated = None

                sources.append(SourceInfo(name=source_name, version=version, records_fetched=records_fetched, errors=errors, last_updated=last_updated))

        return sources

    def build_validation_info(self, validation_results: dict[str, Any] | None = None) -> ValidationInfo:
        """Построить информацию о валидации."""
        if not validation_results:
            return ValidationInfo()

        return ValidationInfo(
            schema_passed=validation_results.get("schema_passed", True),
            qc_passed=validation_results.get("qc_passed", True),
            warnings=validation_results.get("warnings", 0),
            errors=validation_results.get("errors", 0),
        )

    def build_files_info(self, output_files: dict[str, Path]) -> dict[str, str | FileInfo]:
        """Построить информацию о файлах."""
        files_info = {}

        for file_type, file_path in output_files.items():
            if file_type == "checksums":
                # Специальная обработка для checksums
                files_info[file_type] = {}
                if isinstance(file_path, dict):
                    for checksum_type, path in file_path.items():
                        if isinstance(path, Path):
                            files_info[file_type][checksum_type] = self._calculate_file_checksum(path)
                        else:
                            files_info[file_type][checksum_type] = str(path)
            else:
                if isinstance(file_path, Path):
                    files_info[file_type] = self._get_file_info(file_path)
                else:
                    files_info[file_type] = str(file_path)

        return files_info

    def build_metadata(
        self,
        df: Any,
        accepted_df: Any | None = None,
        rejected_df: Any | None = None,
        extraction_results: dict[str, Any] | None = None,
        validation_results: dict[str, Any] | None = None,
        output_files: dict[str, Path] | None = None,
        end_time: datetime | None = None,
        additional_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Построить упрощённые метаданные пайплайна."""

        # Построить основные компоненты
        pipeline_info = self.build_pipeline_info()
        execution_info = self.build_execution_info(end_time)
        data_info = self.build_data_info(df, accepted_df, rejected_df)
        sources_info = self.build_sources_info(extraction_results)
        validation_info = self.build_validation_info(validation_results)
        files_info = self.build_files_info(output_files or {})

        # Вычислить checksums для файлов
        checksums = {}
        for file_type, file_path in files_info.items():
            if isinstance(file_path, (str, Path)):
                path_obj = Path(file_path)
                if path_obj.exists():
                    with open(path_obj, "rb") as f:
                        content = f.read()
                        md5_hash = hashlib.sha256(content).hexdigest()
                        sha256_hash = hashlib.sha256(content).hexdigest()
                        filename = path_obj.name
                        checksums[f"{filename}_md5"] = md5_hash
                        checksums[f"{filename}_sha256"] = sha256_hash

        # Построить упрощённую структуру
        return {
            "pipeline": {"name": self.entity_type, "version": "2.0.0", "entity_type": self.entity_type, "source_system": "chembl"},
            "execution": {
                "run_id": execution_info.run_id,
                "started_at": execution_info.started_at,
                "completed_at": execution_info.completed_at,
                "duration_sec": execution_info.duration_sec,
            },
            "data": {
                "row_count": data_info.row_count,
                "row_count_accepted": data_info.row_count_accepted,
                "row_count_rejected": data_info.row_count_rejected,
                "columns_count": data_info.columns_count,
            },
            "sources": [{"name": source.name, "version": source.version, "records": source.records_fetched} for source in sources_info],
            "validation": {
                "schema_passed": validation_info.schema_passed,
                "qc_passed": validation_info.qc_passed,
                "warnings": validation_info.warnings,
                "errors": validation_info.errors,
            },
            "files": files_info,
            "checksums": checksums,
        }

    def save_metadata(self, metadata: dict, output_path: Path) -> None:
        """Сохранить упрощённые метаданные в YAML файл."""
        # Сохранить в YAML без pickle объектов
        with open(output_path, "w", encoding="utf-8") as f:
            yaml.dump(metadata, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    def load_metadata(self, metadata_path: Path) -> PipelineMetadata:
        """Загрузить метаданные из YAML файла."""
        with open(metadata_path, encoding="utf-8") as f:
            metadata_dict = yaml.safe_load(f)

        return PipelineMetadata(**metadata_dict)


def create_metadata_builder(config: Config, entity_type: str) -> MetadataBuilder:
    """Создать построитель метаданных для типа сущности."""
    return MetadataBuilder(config, entity_type)


def build_standard_metadata(
    config: Config,
    entity_type: str,
    df: Any,
    accepted_df: Any | None = None,
    rejected_df: Any | None = None,
    extraction_results: dict[str, Any] | None = None,
    validation_results: dict[str, Any] | None = None,
    output_files: dict[str, Path] | None = None,
    additional_metadata: dict[str, Any] | None = None,
) -> PipelineMetadata:
    """Построить стандартные метаданные для пайплайна."""
    builder = create_metadata_builder(config, entity_type)
    result = builder.build_metadata(
        df=df,
        accepted_df=accepted_df,
        rejected_df=rejected_df,
        extraction_results=extraction_results,
        validation_results=validation_results,
        output_files=output_files,
        additional_metadata=additional_metadata,
    )
    return PipelineMetadata(**result)
