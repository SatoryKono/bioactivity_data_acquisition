"""
Базовый модуль для записи выходных данных ETL пайплайнов.

Предоставляет единую логику записи выходных файлов для всех пайплайнов.
"""

import hashlib
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field

from library.config import Config

from .error_tracking import ErrorTracker
from .metadata import MetadataBuilder, PipelineMetadata


class ETLResult(BaseModel):
    """Результат выполнения ETL пайплайна."""
    
    model_config = {"arbitrary_types_allowed": True}
    
    # Основные данные
    data: pd.DataFrame = Field(..., description="Основные данные")
    accepted_data: pd.DataFrame | None = Field(None, description="Принятые данные")
    rejected_data: pd.DataFrame | None = Field(None, description="Отклоненные данные")
    
    # QC данные
    qc_summary: pd.DataFrame | None = Field(None, description="QC сводка")
    qc_detailed: pd.DataFrame | None = Field(None, description="QC детальный отчет")
    
    # Корреляционные данные
    correlation_analysis: dict[str, Any] | None = Field(None, description="Корреляционный анализ")
    correlation_reports: dict[str, pd.DataFrame] | None = Field(None, description="Корреляционные отчеты")
    correlation_insights: dict[str, Any] | None = Field(None, description="Корреляционные инсайты")
    
    # Метаданные
    metadata: PipelineMetadata | None = Field(None, description="Метаданные пайплайна")
    
    # Ошибки
    error_tracker: ErrorTracker | None = Field(None, description="Трекер ошибок")
    
    # Дополнительные данные
    additional_data: dict[str, Any] = Field(default_factory=dict, description="Дополнительные данные")


class ETLWriter(ABC):
    """Базовый класс для записи выходных данных ETL пайплайнов."""
    
    def __init__(self, config: Config, entity_type: str):
        self.config = config
        self.entity_type = entity_type
        self.metadata_builder = MetadataBuilder(config, entity_type)
    
    def _ensure_output_directory(self, output_dir: Path) -> None:
        """Создать выходную директорию если не существует."""
        output_dir.mkdir(parents=True, exist_ok=True)
    
    def _calculate_file_checksum(self, file_path: Path) -> str:
        """Вычислить SHA256 хеш файла."""
        if not file_path.exists():
            return ""
        
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return f"sha256:{sha256_hash.hexdigest()}"
    
    def _write_deterministic_csv(
        self,
        df: pd.DataFrame,
        output_path: Path,
        sort_columns: list[str] | None = None,
        ascending: list[bool] | None = None,
        column_order: list[str] | None = None,
        exclude_columns: list[str] | None = None
    ) -> None:
        """Записать DataFrame в CSV с детерминированным порядком."""
        # Создать копию данных
        df_copy = df.copy()
        
        # Исключить служебные колонки
        if exclude_columns is None:
            exclude_columns = ["quality_flag", "quality_reason", "retrieved_at", "_row_id"]
        
        for col in exclude_columns:
            if col in df_copy.columns:
                df_copy = df_copy.drop(columns=[col])
        
        # Упорядочить колонки
        if column_order:
            # Строгий режим: только колонки из column_order
            existing_columns = [col for col in column_order if col in df_copy.columns]
            df_copy = df_copy[existing_columns]
            
            # Логирование для отладки
            missing_in_data = [col for col in column_order if col not in df_copy.columns]
            if missing_in_data:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Columns in column_order but missing in data: {missing_in_data}")
        
        # Сортировка
        if sort_columns:
            # Проверить что все колонки для сортировки существуют
            valid_sort_columns = [col for col in sort_columns if col in df_copy.columns]
            
            if valid_sort_columns:
                # Защита от несоответствия длины ascending и sort_columns
                if ascending is not None and len(ascending) != len(valid_sort_columns):
                    # Если длины не совпадают, использовать True для всех колонок
                    ascending = [True] * len(valid_sort_columns)
                
                df_copy = df_copy.sort_values(by=valid_sort_columns, ascending=ascending)
        
        # Записать в CSV
        df_copy.to_csv(output_path, index=False, encoding='utf-8')
    
    def _write_correlation_reports(
        self,
        correlation_reports: dict[str, pd.DataFrame],
        output_dir: Path,
        date_tag: str
    ) -> dict[str, Path]:
        """Записать корреляционные отчеты."""
        correlation_dir = output_dir / f"{self.entity_type}_correlation_report_{date_tag}"
        correlation_dir.mkdir(exist_ok=True)
        
        written_files = {}
        
        # Записать консолидированный отчет
        if correlation_reports:
            consolidated_data = []
            for report_name, report_df in correlation_reports.items():
                report_df_copy = report_df.copy()
                report_df_copy['report_name'] = report_name
                consolidated_data.append(report_df_copy)
            
            if consolidated_data:
                consolidated_df = pd.concat(consolidated_data, ignore_index=True)
                consolidated_path = correlation_dir / f"{self.entity_type}_correlation_consolidated.csv"
                consolidated_df.to_csv(consolidated_path, index=False, encoding='utf-8')
                written_files["correlation_consolidated"] = consolidated_path
        
        # Записать индивидуальные отчеты
        individual_dir = correlation_dir / "individual_reports"
        individual_dir.mkdir(exist_ok=True)
        
        for report_name, report_df in correlation_reports.items():
            report_path = individual_dir / f"{report_name}.csv"
            report_df.to_csv(report_path, index=False, encoding='utf-8')
            written_files[f"correlation_{report_name}"] = report_path
        
        return written_files
    
    def _write_correlation_insights(
        self,
        correlation_insights: dict[str, Any],
        output_dir: Path,
        date_tag: str
    ) -> Path:
        """Записать корреляционные инсайты."""
        correlation_dir = output_dir / f"{self.entity_type}_correlation_report_{date_tag}"
        correlation_dir.mkdir(exist_ok=True)
        
        insights_path = correlation_dir / "correlation_insights.json"
        with open(insights_path, 'w', encoding='utf-8') as f:
            import json
            json.dump(correlation_insights, f, indent=2, ensure_ascii=False)
        
        return insights_path
    
    def _build_output_files_dict(
        self,
        main_data_path: Path,
        qc_summary_path: Path | None = None,
        metadata_path: Path | None = None,
        correlation_files: dict[str, Path] | None = None,
        correlation_insights_path: Path | None = None
    ) -> dict[str, Any]:
        """Построить словарь выходных файлов."""
        files: dict[str, Any] = {
            "main_data": main_data_path,
        }
        
        if qc_summary_path:
            files["qc_summary"] = qc_summary_path
        
        if metadata_path:
            files["metadata"] = metadata_path
        
        if correlation_files:
            files.update(correlation_files)
        
        if correlation_insights_path:
            files["correlation_insights"] = correlation_insights_path
        
        # Добавить checksums
        checksums: dict[str, str] = {}
        for file_type, file_path in files.items():
            if isinstance(file_path, Path) and file_path.exists():
                checksums[file_type] = self._calculate_file_checksum(file_path)
        
        if checksums:
            files["checksums"] = checksums
        
        return files
    
    @abstractmethod
    def get_sort_columns(self) -> list[str]:
        """Получить колонки для сортировки."""
        pass
    
    @abstractmethod
    def get_column_order(self) -> list[str] | None:
        """Получить порядок колонок."""
        pass
    
    @abstractmethod
    def get_exclude_columns(self) -> list[str]:
        """Получить колонки для исключения из вывода."""
        pass
    
    def write_outputs(
        self,
        result: ETLResult,
        output_dir: Path,
        date_tag: str
    ) -> dict[str, Path]:
        """Записать все выходные файлы."""
        # Создать выходную директорию
        self._ensure_output_directory(output_dir)
        
        # Получить параметры сортировки и колонок
        sort_columns = self.get_sort_columns()
        column_order = self.get_column_order()
        exclude_columns = self.get_exclude_columns()
        
        # Определить ascending для сортировки
        ascending = [True] * len(sort_columns) if sort_columns else None
        
        written_files = {}
        
        # 1. Записать основные данные
        main_data_path = output_dir / f"{self.entity_type}_{date_tag}.csv"
        self._write_deterministic_csv(
            result.data,
            main_data_path,
            sort_columns=sort_columns,
                        ascending=ascending,
            column_order=column_order,
            exclude_columns=exclude_columns
        )
        written_files["main_data"] = main_data_path
        
        # 2. Записать QC сводку (всегда)
        if result.qc_summary is not None:
            qc_summary_path = output_dir / f"{self.entity_type}_{date_tag}_qc_summary.csv"
            result.qc_summary.to_csv(qc_summary_path, index=False, encoding='utf-8')
            written_files["qc_summary"] = qc_summary_path
        
        # 3. Записать корреляционные отчеты (если есть)
        correlation_files = {}
        if result.correlation_reports:
            correlation_files = self._write_correlation_reports(
                result.correlation_reports,
                output_dir,
                date_tag
            )
            written_files.update(correlation_files)
        
        # 4. Записать корреляционные инсайты (если есть)
        if result.correlation_insights:
            correlation_insights_path = self._write_correlation_insights(
                result.correlation_insights,
                output_dir,
                date_tag
            )
            written_files["correlation_insights"] = correlation_insights_path
        
        # 5. Построить и записать метаданные
        if result.metadata is None:
            # Построить метаданные если не предоставлены
            output_files_dict = self._build_output_files_dict(
                main_data_path,
                written_files.get("qc_summary"),
                None,  # metadata_path будет создан ниже
                correlation_files,
                written_files.get("correlation_insights")
            )
            
            result.metadata = self.metadata_builder.build_metadata(
                df=result.data,
                accepted_df=result.accepted_data,
                rejected_df=result.rejected_data,
                output_files=output_files_dict
            )
        
        # Записать метаданные
        metadata_path = output_dir / f"{self.entity_type}_{date_tag}.meta.yaml"
        if result.metadata is not None:
            self.metadata_builder.save_metadata(result.metadata, metadata_path)
        written_files["metadata"] = metadata_path
        
        return written_files


class DocumentETLWriter(ETLWriter):
    """ETL Writer для документов."""
    
    def get_sort_columns(self) -> list[str]:
        """Колонки для сортировки документов."""
        return ["document_chembl_id", "doi", "pmid"]
    
    def get_column_order(self) -> list[str] | None:
        """Порядок колонок для документов."""
        if hasattr(self.config, 'determinism') and hasattr(self.config.determinism, 'column_order'):
            return self.config.determinism.column_order
        return None
    
    def get_exclude_columns(self) -> list[str]:
        """Колонки для исключения из вывода документов."""
        return [
            "quality_flag", "quality_reason", "retrieved_at", "_row_id"
            # Удалено: "index", "hash_row", "hash_business_key"
        ]


class TargetETLWriter(ETLWriter):
    """ETL Writer для таргетов."""
    
    def get_sort_columns(self) -> list[str]:
        """Колонки для сортировки таргетов."""
        return ["target_chembl_id"]
    
    def get_column_order(self) -> list[str] | None:
        """Порядок колонок для таргетов."""
        if hasattr(self.config, 'determinism') and hasattr(self.config.determinism, 'column_order'):
            return self.config.determinism.column_order
        return None
    
    def get_exclude_columns(self) -> list[str]:
        """Колонки для исключения из вывода таргетов."""
        return ["quality_flag", "quality_reason", "retrieved_at", "_row_id"]


class AssayETLWriter(ETLWriter):
    """ETL Writer для ассаев."""
    
    def get_sort_columns(self) -> list[str]:
        """Колонки для сортировки ассаев."""
        return ["assay_chembl_id"]
    
    def get_column_order(self) -> list[str] | None:
        """Порядок колонок для ассаев."""
        if hasattr(self.config, 'determinism') and hasattr(self.config.determinism, 'column_order'):
            return self.config.determinism.column_order
        return None
    
    def get_exclude_columns(self) -> list[str]:
        """Колонки для исключения из вывода ассаев."""
        return ["quality_flag", "quality_reason", "retrieved_at", "_row_id"]


class ActivityETLWriter(ETLWriter):
    """ETL Writer для активностей."""
    
    def get_sort_columns(self) -> list[str]:
        """Колонки для сортировки активностей."""
        return [
            "activity_chembl_id",
            "assay_chembl_id", 
            "document_chembl_id",
            "molecule_chembl_id",
            "target_chembl_id"
        ]
    
    def get_column_order(self) -> list[str] | None:
        """Порядок колонок для активностей."""
        if hasattr(self.config, 'determinism') and hasattr(self.config.determinism, 'column_order'):
            return self.config.determinism.column_order
        return None
    
    def get_exclude_columns(self) -> list[str]:
        """Колонки для исключения из вывода активностей."""
        return ["quality_flag", "quality_reason", "retrieved_at", "_row_id"]


class TestitemETLWriter(ETLWriter):
    """ETL Writer для теститемов."""
    
    def get_sort_columns(self) -> list[str]:
        """Колонки для сортировки теститемов."""
        return ["molecule_chembl_id"]
    
    def get_column_order(self) -> list[str] | None:
        """Порядок колонок для теститемов."""
        if hasattr(self.config, 'determinism') and hasattr(self.config.determinism, 'column_order'):
            return self.config.determinism.column_order
        return None
    
    def get_exclude_columns(self) -> list[str]:
        """Колонки для исключения из вывода теститемов."""
        return ["quality_flag", "quality_reason", "retrieved_at", "_row_id"]


def create_etl_writer(config: Config, entity_type: str) -> ETLWriter:
    """Создать ETL Writer для типа сущности."""
    if entity_type == "documents":
        return DocumentETLWriter(config, entity_type)
    elif entity_type == "targets":
        return TargetETLWriter(config, entity_type)
    elif entity_type == "assays":
        return AssayETLWriter(config, entity_type)
    elif entity_type == "activities":
        return ActivityETLWriter(config, entity_type)
    elif entity_type == "testitems":
        return TestitemETLWriter(config, entity_type)
    else:
        raise ValueError(f"Неподдерживаемый тип сущности: {entity_type}")


def write_etl_outputs(
    result: ETLResult,
    output_dir: Path,
    date_tag: str,
    config: Config,
    entity_type: str
) -> dict[str, Path]:
    """Записать выходные файлы ETL пайплайна."""
    writer = create_etl_writer(config, entity_type)
    return writer.write_outputs(result, output_dir, date_tag)