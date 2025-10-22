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
    
    @abstractmethod
    def _setup_clients(self) -> None:
        """Инициализация HTTP клиентов.
        
        Должен быть реализован в каждом пайплайне для настройки
        специфичных для домена API клиентов.
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
        
        # 4. Фильтрация по качеству
        logger.info("Filtering by quality")
        accepted_data, rejected_data = self.filter_quality(validated_data)
        logger.info(f"Accepted {len(accepted_data)} records, rejected {len(rejected_data)} records")
        
        # 5. Генерация QC отчетов
        logger.info("Building QC reports")
        qc_summary, qc_detailed = self.build_qc_report(accepted_data)
        
        # 6. Построение метаданных
        logger.info("Building metadata")
        metadata = self._build_metadata(accepted_data)
        
        # 7. Корреляционный анализ (опционально)
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
