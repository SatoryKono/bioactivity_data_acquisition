"""Base classes for ETL pipelines."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

import pandas as pd

from library.etl.enhanced_correlation import (
    build_correlation_insights,
    build_enhanced_correlation_analysis,
    build_enhanced_correlation_reports,
    prepare_data_for_correlation_analysis,
)
from library.etl.enhanced_qc import build_enhanced_qc_detailed, build_enhanced_qc_summary

# Импорты новых унифицированных модулей
from .error_tracking import ErrorTracker, ErrorType, ErrorSeverity
from .metadata import MetadataBuilder
from .qc_profiles import QCValidator, QCProfile
from .postprocess_base import BasePostprocessor
from .writer_base import ETLResult as NewETLResult, ETLWriter

logger = logging.getLogger(__name__)

T = TypeVar('T')  # Тип конфигурации


@dataclass(slots=True)
class ETLResult:
    """Контейнер результатов ETL.
    
    Стандартизированная структура для всех пайплайнов,
    обеспечивающая единообразие выходных данных.
    """
    
    data: pd.DataFrame
    """Основные данные (принятые записи)."""
    
    qc_summary: pd.DataFrame
    """Сводный QC отчет."""
    
    qc_detailed: pd.DataFrame | None = None
    """Детальный QC отчет (опционально)."""
    
    rejected: pd.DataFrame | None = None
    """Отклоненные записи (опционально)."""
    
    meta: dict[str, Any] | None = None
    """Метаданные пайплайна."""
    
    correlation_analysis: dict[str, Any] | None = None
    """Корреляционный анализ (опционально)."""
    
    correlation_reports: dict[str, pd.DataFrame] | None = None
    """Корреляционные отчеты (опционально)."""
    
    correlation_insights: list[dict[str, Any]] | None = None
    """Корреляционные инсайты (опционально)."""


class PipelineBase(ABC, Generic[T]):
    """Базовый класс для всех ETL пайплайнов.
    
    Обеспечивает единообразную структуру и интерфейс для всех пайплайнов,
    устраняя дублирование кода и стандартизируя процесс обработки данных.
    
    Типичный workflow:
    1. extract() - извлечение данных из API
    2. normalize() - нормализация данных
    3. validate() - валидация данных
    4. filter_quality() - фильтрация по качеству
    5. build_qc_report() - генерация QC отчетов
    6. _build_metadata() - построение метаданных
    7. _build_correlation() - корреляционный анализ (опционально)
    """
    
    def __init__(self, config: T) -> None:
        """Инициализация пайплайна.
        
        Args:
            config: Конфигурация пайплайна
        """
        self.config = config
        self._setup_clients()
        
        # Инициализация новых унифицированных компонентов
        # Эти компоненты будут инициализированы в дочерних классах
        self.error_tracker: ErrorTracker | None = None
        self.metadata_builder: MetadataBuilder | None = None
        self.qc_validator: QCValidator | None = None
        self.postprocessor: BasePostprocessor | None = None
        self.etl_writer: ETLWriter | None = None
    
    def _initialize_unified_components(self) -> None:
        """Инициализация унифицированных компонентов.
        
        Должен быть вызван в дочерних классах после реализации абстрактных методов.
        """
        self.error_tracker = ErrorTracker(self._get_entity_type())
        self.metadata_builder = MetadataBuilder(self.config, self._get_entity_type())
        self.qc_validator = self._create_qc_validator()
        self.postprocessor = self._create_postprocessor()
        self.etl_writer = self._create_etl_writer()
    
    @abstractmethod
    def _setup_clients(self) -> None:
        """Инициализация HTTP клиентов.
        
        Должен быть реализован в каждом пайплайне для настройки
        специфичных для домена API клиентов.
        """
        pass
    
    @abstractmethod
    def _get_entity_type(self) -> str:
        """Получить тип сущности для пайплайна.
        
        Returns:
            Тип сущности (например, 'documents', 'targets', 'assays')
        """
        pass
    
    @abstractmethod
    def _create_qc_validator(self) -> QCValidator:
        """Создать QC валидатор для пайплайна.
        
        Returns:
            Настроенный QC валидатор
        """
        pass
    
    @abstractmethod
    def _create_postprocessor(self) -> BasePostprocessor:
        """Создать постпроцессор для пайплайна.
        
        Returns:
            Настроенный постпроцессор
        """
        pass
    
    @abstractmethod
    def _create_etl_writer(self) -> ETLWriter:
        """Создать ETL writer для пайплайна.
        
        Returns:
            Настроенный ETL writer
        """
        pass
    
    @abstractmethod
    def extract(self, input_data: pd.DataFrame) -> pd.DataFrame:
        """Извлечение данных из API.
        
        Args:
            input_data: Входные данные (обычно список идентификаторов)
            
        Returns:
            Сырые данные из API
        """
        pass
    
    @abstractmethod
    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Нормализация данных.
        
        Args:
            raw_data: Сырые данные из API
            
        Returns:
            Нормализованные данные
        """
        pass
    
    @abstractmethod
    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        """Валидация данных.
        
        Args:
            data: Данные для валидации
            
        Returns:
            Валидированные данные
        """
        pass
    
    def filter_quality(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Фильтрация по качеству.
        
        Разделяет данные на принятые и отклоненные на основе
        настроек контроля качества.
        
        Args:
            data: Данные для фильтрации
            
        Returns:
            Кортеж (accepted_data, rejected_data)
        """
        # Базовая реализация - все данные принимаются
        # Пайплайны могут переопределить для специфичной логики
        return data, pd.DataFrame()
    
    def build_qc_report(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame | None]:
        """Генерация QC отчетов.
        
        Создает сводный и детальный QC отчеты используя
        общие утилиты из library.etl.enhanced_qc.
        
        Args:
            data: Данные для анализа
            
        Returns:
            Кортеж (qc_summary, qc_detailed)
        """
        if data.empty:
            logger.warning("DataFrame is empty, returning empty QC report")
            return pd.DataFrame([{"metric": "row_count", "value": 0}]), None
        
        try:
            # Сводный QC отчет
            qc_summary = build_enhanced_qc_summary(data)
            
            # Детальный QC отчет (опционально)
            qc_detailed = None
            if hasattr(self.config, 'postprocess') and hasattr(self.config.postprocess, 'qc'):
                qc_config = self.config.postprocess.qc
                if getattr(qc_config, 'detailed_report', False):
                    qc_detailed = build_enhanced_qc_detailed(data)
            
            return qc_summary, qc_detailed
            
        except Exception as e:
            logger.error(f"Failed to build QC report: {e}")
            # Fallback к простому отчету
            return pd.DataFrame([{"metric": "row_count", "value": len(data)}]), None
    
    def _should_build_correlation(self) -> bool:
        """Проверка, нужен ли корреляционный анализ.
        
        Returns:
            True если корреляционный анализ включен в конфигурации
        """
        try:
            return (
                hasattr(self.config, 'postprocess') and
                hasattr(self.config.postprocess, 'correlation') and
                getattr(self.config.postprocess.correlation, 'enabled', False)
            )
        except AttributeError:
            return False
    
    def _build_correlation(self, data: pd.DataFrame) -> tuple[dict[str, Any], dict[str, pd.DataFrame] | None, list[dict[str, Any]] | None]:
        """Построение корреляционного анализа.
        
        Использует общие утилиты из library.etl.enhanced_correlation.
        
        Args:
            data: Данные для анализа
            
        Returns:
            Кортеж (correlation_analysis, correlation_reports, correlation_insights)
        """
        if not self._should_build_correlation():
            return None, None, None
        
        try:
            # Подготовка данных для корреляционного анализа
            prepared_data = prepare_data_for_correlation_analysis(data)
            
            # Корреляционный анализ
            correlation_analysis = build_enhanced_correlation_analysis(prepared_data)
            
            # Корреляционные отчеты
            correlation_reports = build_enhanced_correlation_reports(prepared_data)
            
            # Корреляционные инсайты
            correlation_insights = build_correlation_insights(correlation_analysis)
            
            return correlation_analysis, correlation_reports, correlation_insights
            
        except Exception as e:
            logger.error(f"Failed to build correlation analysis: {e}")
            return None, None, None
    
    @abstractmethod
    def _build_metadata(self, data: pd.DataFrame) -> dict[str, Any]:
        """Построение метаданных.
        
        Args:
            data: Обработанные данные
            
        Returns:
            Словарь метаданных
        """
        pass
    
    def _track_extraction_error(self, source: str, message: str, details: dict[str, Any] | None = None) -> None:
        """Отследить ошибку извлечения.
        
        Args:
            source: Источник ошибки
            message: Сообщение об ошибке
            details: Дополнительные детали
        """
        if self.error_tracker is not None:
            self.error_tracker.add_error(
                error_type=ErrorType.EXTRACTION,
                source=source,
                message=message,
                severity=ErrorSeverity.HIGH,
                details=details
            )
    
    def _track_validation_error(self, source: str, message: str, record_id: str | None = None, details: dict[str, Any] | None = None) -> None:
        """Отследить ошибку валидации.
        
        Args:
            source: Источник ошибки
            message: Сообщение об ошибке
            record_id: ID записи
            details: Дополнительные детали
        """
        if self.error_tracker is not None:
            self.error_tracker.add_error(
                error_type=ErrorType.VALIDATION,
                source=source,
                message=message,
                severity=ErrorSeverity.MEDIUM,
                record_id=record_id,
                details=details
            )
    
    def _track_transformation_error(self, source: str, message: str, details: dict[str, Any] | None = None) -> None:
        """Отследить ошибку трансформации.
        
        Args:
            source: Источник ошибки
            message: Сообщение об ошибке
            details: Дополнительные детали
        """
        if self.error_tracker is not None:
            self.error_tracker.add_error(
                error_type=ErrorType.TRANSFORMATION,
                source=source,
                message=message,
                severity=ErrorSeverity.MEDIUM,
                details=details
            )
    
    def _track_load_error(self, source: str, message: str, details: dict[str, Any] | None = None) -> None:
        """Отследить ошибку загрузки.
        
        Args:
            source: Источник ошибки
            message: Сообщение об ошибке
            details: Дополнительные детали
        """
        if self.error_tracker is not None:
            self.error_tracker.add_error(
                error_type=ErrorType.LOAD,
                source=source,
                message=message,
                severity=ErrorSeverity.HIGH,
                details=details
            )
    
    def _validate_data_quality(self, data: pd.DataFrame) -> dict[str, Any]:
        """Выполнить валидацию качества данных.
        
        Args:
            data: Данные для валидации
            
        Returns:
            Результаты валидации
        """
        if self.qc_validator is not None:
            return self.qc_validator.validate(data)
        return {}
    
    def _apply_postprocessing(self, data: pd.DataFrame) -> pd.DataFrame:
        """Применить постобработку данных.
        
        Args:
            data: Данные для постобработки
            
        Returns:
            Обработанные данные
        """
        if self.postprocessor is not None:
            return self.postprocessor.apply_steps(data)
        return data
    
    def run(self, input_data: pd.DataFrame) -> ETLResult:
        """Основной метод запуска пайплайна.
        
        Выполняет полный цикл ETL:
        1. Извлечение данных
        2. Нормализация
        3. Валидация
        4. Фильтрация по качеству
        5. Генерация QC отчетов
        6. Построение метаданных
        7. Корреляционный анализ (опционально)
        
        Args:
            input_data: Входные данные
            
        Returns:
            Результат ETL процесса
        """
        logger.info(f"Starting ETL pipeline with {len(input_data)} input records")
        
        try:
            # 1. Извлечение данных
            logger.info("Extracting data from APIs")
            raw_data = self.extract(input_data)
            logger.info(f"Extracted {len(raw_data)} raw records")
            
            # 2. Нормализация
            logger.info("Normalizing data")
            normalized_data = self.normalize(raw_data)
            logger.info(f"Normalized {len(normalized_data)} records")
            
            # 3. Валидация
            logger.info("Validating data")
            validated_data = self.validate(normalized_data)
            logger.info(f"Validated {len(validated_data)} records")
            
            # 4. Постобработка
            logger.info("Applying postprocessing")
            processed_data = self._apply_postprocessing(validated_data)
            logger.info(f"Postprocessed {len(processed_data)} records")
            
            # 5. Валидация качества данных
            logger.info("Validating data quality")
            quality_results = self._validate_data_quality(processed_data)
            logger.info(f"Quality validation completed: {quality_results}")
            
            # 6. Фильтрация по качеству
            logger.info("Filtering by quality")
            accepted_data, rejected_data = self.filter_quality(processed_data)
            logger.info(f"Accepted {len(accepted_data)} records, rejected {len(rejected_data)} records")
            
            # 7. Генерация QC отчетов
            logger.info("Building QC reports")
            qc_summary, qc_detailed = self.build_qc_report(accepted_data)
            
            # 8. Построение метаданных
            logger.info("Building metadata")
            metadata = self._build_metadata(accepted_data)
            
            # 9. Корреляционный анализ (опционально)
            correlation_analysis = None
            correlation_reports = None
            correlation_insights = None
            
            if self._should_build_correlation():
                logger.info("Building correlation analysis")
                correlation_analysis, correlation_reports, correlation_insights = self._build_correlation(accepted_data)
            
            logger.info("ETL pipeline completed successfully")
            
            return ETLResult(
                data=accepted_data,
                qc_summary=qc_summary,
                qc_detailed=qc_detailed,
                rejected=rejected_data if not rejected_data.empty else None,
                meta=metadata,
                correlation_analysis=correlation_analysis,
                correlation_reports=correlation_reports,
                correlation_insights=correlation_insights,
            )
        
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            self._track_load_error("pipeline", f"Pipeline execution failed: {str(e)}", {"exception": str(e)})
            raise
    
    def run_unified(self, input_data: pd.DataFrame) -> NewETLResult:
        """Запуск пайплайна с использованием новых унифицированных компонентов.
        
        Args:
            input_data: Входные данные
            
        Returns:
            Унифицированный результат ETL
        """
        logger.info(f"Starting unified ETL pipeline with {len(input_data)} input records")
        
        try:
            # 1. Извлечение данных
            logger.info("Extracting data from APIs")
            raw_data = self.extract(input_data)
            logger.info(f"Extracted {len(raw_data)} raw records")
            
            # 2. Нормализация
            logger.info("Normalizing data")
            normalized_data = self.normalize(raw_data)
            logger.info(f"Normalized {len(normalized_data)} records")
            
            # 3. Валидация
            logger.info("Validating data")
            validated_data = self.validate(normalized_data)
            logger.info(f"Validated {len(validated_data)} records")
            
            # 4. Постобработка
            logger.info("Applying postprocessing")
            processed_data = self._apply_postprocessing(validated_data)
            logger.info(f"Postprocessed {len(processed_data)} records")
            
            # 5. Валидация качества данных
            logger.info("Validating data quality")
            quality_results = self._validate_data_quality(processed_data)
            logger.info(f"Quality validation completed: {quality_results}")
            
            # 6. Фильтрация по качеству
            logger.info("Filtering by quality")
            accepted_data, rejected_data = self.filter_quality(processed_data)
            logger.info(f"Accepted {len(accepted_data)} records, rejected {len(rejected_data)} records")
            
            # 7. Генерация QC отчетов
            logger.info("Building QC reports")
            qc_summary, qc_detailed = self.build_qc_report(accepted_data)
            
            # 8. Корреляционный анализ (опционально)
            correlation_analysis = None
            correlation_reports = None
            correlation_insights = None
            
            if self._should_build_correlation():
                logger.info("Building correlation analysis")
                correlation_analysis, correlation_reports, correlation_insights = self._build_correlation(accepted_data)
            
            # 9. Построение метаданных
            logger.info("Building metadata")
            if self.metadata_builder is not None:
                metadata = self.metadata_builder.build_metadata(
                    df=accepted_data,
                    accepted_df=accepted_data,
                    rejected_df=rejected_data if not rejected_data.empty else None,
                    qc_summary=qc_summary,
                    error_tracker=self.error_tracker,
                    custom_metadata=quality_results
                )
            else:
                metadata = None
            
            logger.info("Unified ETL pipeline completed successfully")
            
            return NewETLResult(
                data=accepted_data,
                accepted_data=accepted_data,
                rejected_data=rejected_data if not rejected_data.empty else None,
                qc_summary=qc_summary,
                qc_detailed=qc_detailed,
                correlation_analysis=correlation_analysis,
                correlation_reports=correlation_reports,
                correlation_insights=correlation_insights,
                metadata=metadata,
                error_tracker=self.error_tracker,
                additional_data=quality_results
            )
        
        except Exception as e:
            logger.error(f"Unified ETL pipeline failed: {e}")
            self._track_load_error("pipeline", f"Unified pipeline execution failed: {str(e)}", {"exception": str(e)})
            raise
