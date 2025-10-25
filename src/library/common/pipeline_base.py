"""Base classes for ETL pipelines."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import pandas as pd

if TYPE_CHECKING:
    from library.config import Config

# Lazy imports to avoid circular dependency with library.etl
# These will be imported inside methods where they're used
from library.etl.enhanced_qc import build_enhanced_qc_summary, build_enhanced_qc_detailed

# Импорты новых унифицированных модулей
from .error_tracking import ErrorSeverity, ErrorTracker, ErrorType
from .metadata import MetadataBuilder, PipelineMetadata
from .postprocess_base import BasePostprocessor
from .qc_profiles import QCValidator
from .writer_base import ETLResult, ETLWriter

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="Config")  # Тип конфигурации


# ETLResult теперь импортируется из writer_base


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
        """Создаёт базовый QC отчёт.

        Генерирует как сводный, так и детальный QC отчёты с использованием
        функций из library.etl.enhanced_qc.
        """
        # Lazy import to avoid circular dependency
        # from library.etl.enhanced_qc import build_enhanced_qc_detailed

        # qc_summary = build_enhanced_qc_summary(data)
        qc_summary = {}
        qc_detailed = None

        # Проверяем, нужен ли детальный отчёт
        if hasattr(self.config, "postprocess") and self.config.postprocess:
            qc_config = self.config.postprocess.qc
            if getattr(qc_config, "detailed_report", False):
                qc_detailed = build_enhanced_qc_detailed(data)

        return qc_summary, qc_detailed

    def _should_build_correlation(self) -> bool:
        """Проверка, нужен ли корреляционный анализ.

        Returns:
            True если корреляционный анализ включен в конфигурации
        """
        try:
            return hasattr(self.config, "postprocess") and hasattr(self.config.postprocess, "correlation") and getattr(self.config.postprocess.correlation, "enabled", False)
        except AttributeError:
            return False

    def _build_correlation(self, data: pd.DataFrame) -> tuple[dict[str, Any] | None, dict[str, pd.DataFrame] | None, dict[str, Any] | None]:
        """Создаёт корреляционный анализ данных.

        Все вспомогательные функции импортируются из library.etl.enhanced_correlation.

        Args:
            data: Данные для анализа

        Returns:
            Кортеж (correlation_analysis, correlation_reports, correlation_insights)
        """
        # Lazy imports to avoid circular dependency
        from library.etl.enhanced_correlation import (
            build_correlation_insights,
            build_enhanced_correlation_analysis,
            build_enhanced_correlation_reports,
            prepare_data_for_correlation_analysis,
        )

        entity_type = self._get_entity_type()
        prepared_data = prepare_data_for_correlation_analysis(data, entity_type)

        # Выполняем корреляционный анализ
        correlation_analysis = build_enhanced_correlation_analysis(prepared_data, logger=logger)

        # Создаём корреляционные отчёты
        correlation_reports = build_enhanced_correlation_reports(prepared_data, logger=logger)

        # Создаём корреляционные insights
        correlation_insights_list = build_correlation_insights(prepared_data, logger=logger)
        correlation_insights = {"insights": correlation_insights_list} if correlation_insights_list else None

        return correlation_analysis, correlation_reports, correlation_insights

    def _build_metadata(
        self,
        data: pd.DataFrame,
        accepted_data: pd.DataFrame | None = None,
        rejected_data: pd.DataFrame | None = None,
        correlation_analysis: dict[str, Any] | None = None,
        correlation_insights: dict[str, Any] | None = None,
    ) -> PipelineMetadata:
        """Построение метаданных.

        Args:
            data: Обработанные данные
            accepted_data: Принятые данные
            rejected_data: Отклоненные данные
            correlation_analysis: Корреляционный анализ
            correlation_insights: Корреляционные инсайты

        Returns:
            Метаданные пайплайна
        """
        from .metadata import MetadataBuilder

        # Создаем построитель метаданных
        builder = MetadataBuilder(self.config, self._get_entity_type())

        # Подготовить дополнительные метаданные
        additional_metadata = {}
        if correlation_analysis is not None:
            additional_metadata["correlation_analysis"] = correlation_analysis
        if correlation_insights is not None:
            additional_metadata["correlation_insights"] = correlation_insights

        # Строим метаданные
        result = builder.build_metadata(
            df=data, accepted_df=accepted_data, rejected_df=rejected_data, end_time=datetime.now(), additional_metadata=additional_metadata if additional_metadata else None
        )
        return PipelineMetadata(**result)

    def _track_extraction_error(self, source: str, message: str, details: dict[str, Any] | None = None) -> None:
        """Отследить ошибку извлечения.

        Args:
            source: Источник ошибки
            message: Сообщение об ошибке
            details: Дополнительные детали
        """
        if self.error_tracker is not None:
            self.error_tracker.add_error(error_type=ErrorType.EXTRACTION, source=source, message=message, severity=ErrorSeverity.HIGH, details=details)

    def _track_validation_error(self, source: str, message: str, record_id: str | None = None, details: dict[str, Any] | None = None) -> None:
        """Отследить ошибку валидации.

        Args:
            source: Источник ошибки
            message: Сообщение об ошибке
            record_id: ID записи
            details: Дополнительные детали
        """
        if self.error_tracker is not None:
            self.error_tracker.add_error(error_type=ErrorType.VALIDATION, source=source, message=message, severity=ErrorSeverity.MEDIUM, record_id=record_id, details=details)

    def _track_transformation_error(self, source: str, message: str, details: dict[str, Any] | None = None) -> None:
        """Отследить ошибку трансформации.

        Args:
            source: Источник ошибки
            message: Сообщение об ошибке
            details: Дополнительные детали
        """
        if self.error_tracker is not None:
            self.error_tracker.add_error(error_type=ErrorType.TRANSFORMATION, source=source, message=message, severity=ErrorSeverity.MEDIUM, details=details)

    def _track_load_error(self, source: str, message: str, details: dict[str, Any] | None = None) -> None:
        """Отследить ошибку загрузки.

        Args:
            source: Источник ошибки
            message: Сообщение об ошибке
            details: Дополнительные детали
        """
        if self.error_tracker is not None:
            self.error_tracker.add_error(error_type=ErrorType.LOAD, source=source, message=message, severity=ErrorSeverity.HIGH, details=details)

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
            return self.postprocessor.process(data)
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

            # 8. Корреляционный анализ (опционально)
            correlation_analysis = None
            correlation_reports = None
            correlation_insights = None

            if self._should_build_correlation():
                logger.info("Building correlation analysis")
                correlation_analysis, correlation_reports, correlation_insights = self._build_correlation(accepted_data)

            # 9. Построение метаданных
            logger.info("Building metadata")
            metadata = self._build_metadata(accepted_data, accepted_data, rejected_data, correlation_analysis=correlation_analysis, correlation_insights=correlation_insights)

            logger.info("ETL pipeline completed successfully")

            return ETLResult(
                data=accepted_data,
                qc_summary=qc_summary,
                qc_detailed=qc_detailed,
                rejected_data=rejected_data if rejected_data is not None and not rejected_data.empty else None,
                metadata=metadata,
                correlation_analysis=correlation_analysis,
                correlation_reports=correlation_reports,
                correlation_insights=correlation_insights,
                error_tracker=self.error_tracker,
            )

        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            self._track_load_error("pipeline", f"Pipeline execution failed: {str(e)}", {"exception": str(e)})
            raise

    def run_unified(self, input_data: pd.DataFrame) -> ETLResult:
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
                # Подготовить дополнительные метаданные
                additional_metadata = {"qc_summary": qc_summary}
                if correlation_analysis is not None:
                    additional_metadata["correlation_analysis"] = correlation_analysis
                if correlation_insights is not None:
                    additional_metadata["correlation_insights"] = correlation_insights

                metadata = self.metadata_builder.build_metadata(
                    df=accepted_data,
                    accepted_df=accepted_data,
                    rejected_df=rejected_data if not rejected_data.empty else None,
                    validation_results=quality_results,
                    additional_metadata=additional_metadata,
                )
            else:
                metadata = None

            logger.info("Unified ETL pipeline completed successfully")

            return ETLResult(
                data=accepted_data,
                accepted_data=accepted_data,
                rejected_data=rejected_data if rejected_data is not None and not rejected_data.empty else None,
                qc_summary=qc_summary,
                qc_detailed=qc_detailed,
                correlation_analysis=correlation_analysis,
                correlation_reports=correlation_reports,
                correlation_insights=correlation_insights,
                metadata=metadata,
                error_tracker=self.error_tracker,
                additional_data=quality_results,
            )

        except Exception as e:
            logger.error(f"Unified ETL pipeline failed: {e}")
            self._track_load_error("pipeline", f"Unified pipeline execution failed: {str(e)}", {"exception": str(e)})
            raise
