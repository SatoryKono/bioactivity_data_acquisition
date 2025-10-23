"""
Модуль для отслеживания ошибок в ETL пайплайнах.

Предоставляет стандартизированный формат ошибок и их отслеживание.
"""

import json
from datetime import datetime
from enum import Enum
from typing import Any
import pandas as pd
from pydantic import BaseModel, Field


class ErrorType(str, Enum):
    """Типы ошибок."""
    
    EXTRACTION_ERROR = "extraction_error"
    VALIDATION_ERROR = "validation_error"
    TRANSFORMATION_ERROR = "transformation_error"
    NETWORK_ERROR = "network_error"
    TIMEOUT_ERROR = "timeout_error"
    SCHEMA_ERROR = "schema_error"
    QC_ERROR = "qc_error"
    UNKNOWN_ERROR = "unknown_error"


class ErrorSeverity(str, Enum):
    """Уровни серьезности ошибок."""
    
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorInfo(BaseModel):
    """Информация об ошибке."""
    
    error_type: ErrorType = Field(..., description="Тип ошибки")
    severity: ErrorSeverity = Field(ErrorSeverity.MEDIUM, description="Уровень серьезности")
    source: str = Field(..., description="Источник ошибки")
    message: str = Field(..., description="Сообщение об ошибке")
    details: dict[str, Any] | None = Field(None, description="Дополнительные детали")
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat(), description="Время ошибки")
    record_id: str | None = Field(None, description="ID записи, связанной с ошибкой")
    stack_trace: str | None = Field(None, description="Стек вызовов")


class ExtractionStatus(str, Enum):
    """Статус извлечения данных."""
    
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


class ErrorTracker:
    """Трекер ошибок для ETL пайплайнов."""
    
    def __init__(self, entity_type: str):
        self.entity_type = entity_type
        self.errors: list[ErrorInfo] = []
        self.extraction_status: dict[str, ExtractionStatus] = {}
        self.error_counts: dict[str, int] = {}
    
    def add_error(
        self,
        error_type: ErrorType,
        source: str,
        message: str,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        details: dict[str, Any] | None = None,
        record_id: str | None = None,
        stack_trace: str | None = None
    ) -> None:
        """Добавить ошибку в трекер."""
        error = ErrorInfo(
            error_type=error_type,
            severity=severity,
            source=source,
            message=message,
            details=details,
            record_id=record_id,
            stack_trace=stack_trace
        )
        
        self.errors.append(error)
        
        # Обновить счетчики
        error_key = f"{error_type.value}_{source}"
        self.error_counts[error_key] = self.error_counts.get(error_key, 0) + 1
    
    def set_extraction_status(self, source: str, status: ExtractionStatus) -> None:
        """Установить статус извлечения для источника."""
        self.extraction_status[source] = status
    
    def get_errors_by_type(self, error_type: ErrorType) -> list[ErrorInfo]:
        """Получить ошибки по типу."""
        return [error for error in self.errors if error.error_type == error_type]
    
    def get_errors_by_source(self, source: str) -> list[ErrorInfo]:
        """Получить ошибки по источнику."""
        return [error for error in self.errors if error.source == source]
    
    def get_errors_by_severity(self, severity: ErrorSeverity) -> list[ErrorInfo]:
        """Получить ошибки по уровню серьезности."""
        return [error for error in self.errors if error.severity == severity]
    
    def get_error_summary(self) -> dict[str, Any]:
        """Получить сводку по ошибкам."""
        summary = {
            "total_errors": len(self.errors),
            "errors_by_type": {},
            "errors_by_source": {},
            "errors_by_severity": {},
            "extraction_status": dict(self.extraction_status),
            "error_counts": dict(self.error_counts)
        }
        
        # Группировка по типам
        for error in self.errors:
            error_type = error.error_type.value
            summary["errors_by_type"][error_type] = summary["errors_by_type"].get(error_type, 0) + 1
        
        # Группировка по источникам
        for error in self.errors:
            source = error.source
            summary["errors_by_source"][source] = summary["errors_by_source"].get(source, 0) + 1
        
        # Группировка по серьезности
        for error in self.errors:
            severity = error.severity.value
            summary["errors_by_severity"][severity] = summary["errors_by_severity"].get(severity, 0) + 1
        
        return summary
    
    def to_json(self) -> str:
        """Конвертировать ошибки в JSON."""
        return json.dumps({
            "entity_type": self.entity_type,
            "errors": [error.model_dump() for error in self.errors],
            "extraction_status": {k: v.value for k, v in self.extraction_status.items()},
            "error_counts": self.error_counts,
            "summary": self.get_error_summary()
        }, indent=2, ensure_ascii=False)
    
    def clear_errors(self) -> None:
        """Очистить все ошибки."""
        self.errors.clear()
        self.extraction_status.clear()
        self.error_counts.clear()


class DataFrameErrorTracker:
    """Трекер ошибок для DataFrame с добавлением колонок ошибок."""
    
    def __init__(self, entity_type: str):
        self.tracker = ErrorTracker(entity_type)
        self.entity_type = entity_type
    
    def add_extraction_errors_column(self, df: pd.DataFrame, source: str, errors: dict[str, str]) -> pd.DataFrame:
        """Добавить колонку ошибок извлечения."""
        df = df.copy()
        error_column = f"{source}_error"
        
        # Создать колонку ошибок
        df[error_column] = None
        
        # Заполнить ошибки
        for record_id, error_message in errors.items():
            if record_id in df.index:
                df.loc[record_id, error_column] = error_message
                
                # Добавить в трекер
                self.tracker.add_error(
                    error_type=ErrorType.EXTRACTION_ERROR,
                    source=source,
                    message=error_message,
                    record_id=str(record_id)
                )
        
        return df
    
    def add_validation_errors_column(self, df: pd.DataFrame, validation_errors: dict[str, list[str]]) -> pd.DataFrame:
        """Добавить колонку ошибок валидации."""
        df = df.copy()
        
        # Создать колонку ошибок валидации
        df["validation_errors"] = None
        
        # Заполнить ошибки валидации
        for record_id, errors in validation_errors.items():
            if record_id in df.index:
                error_json = json.dumps(errors, ensure_ascii=False)
                df.loc[record_id, "validation_errors"] = error_json
                
                # Добавить в трекер
                for error in errors:
                    self.tracker.add_error(
                        error_type=ErrorType.VALIDATION_ERROR,
                        source="validation",
                        message=error,
                        record_id=str(record_id)
                    )
        
        return df
    
    def add_extraction_status_column(self, df: pd.DataFrame, source: str, status: ExtractionStatus) -> pd.DataFrame:
        """Добавить колонку статуса извлечения."""
        df = df.copy()
        status_column = f"{source}_status"
        
        # Создать колонку статуса
        df[status_column] = status.value
        
        # Обновить статус в трекере
        self.tracker.set_extraction_status(source, status)
        
        return df
    
    def add_global_error_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Добавить глобальные колонки ошибок."""
        df = df.copy()
        
        # Колонка с общим статусом извлечения
        df["extraction_status"] = ExtractionStatus.SUCCESS.value
        
        # Колонка с ошибками извлечения (JSON)
        df["extraction_errors"] = None
        
        # Колонка с ошибками валидации (JSON)
        df["validation_errors"] = None
        
        # Обновить статус на PARTIAL если есть ошибки
        if self.tracker.errors:
            has_critical_errors = any(
                error.severity == ErrorSeverity.CRITICAL 
                for error in self.tracker.errors
            )
            
            if has_critical_errors:
                df["extraction_status"] = ExtractionStatus.FAILED.value
            else:
                df["extraction_status"] = ExtractionStatus.PARTIAL.value
        
        # Заполнить ошибки извлечения
        extraction_errors = {}
        for error in self.tracker.get_errors_by_type(ErrorType.EXTRACTION_ERROR):
            if error.record_id:
                if error.record_id not in extraction_errors:
                    extraction_errors[error.record_id] = []
                extraction_errors[error.record_id].append({
                    "source": error.source,
                    "error_type": error.error_type.value,
                    "message": error.message,
                    "timestamp": error.timestamp
                })
        
        # Заполнить ошибки валидации
        validation_errors = {}
        for error in self.tracker.get_errors_by_type(ErrorType.VALIDATION_ERROR):
            if error.record_id:
                if error.record_id not in validation_errors:
                    validation_errors[error.record_id] = []
                validation_errors[error.record_id].append({
                    "error_type": error.error_type.value,
                    "message": error.message,
                    "timestamp": error.timestamp
                })
        
        # Применить ошибки к DataFrame
        for record_id, errors in extraction_errors.items():
            if record_id in df.index:
                df.loc[record_id, "extraction_errors"] = json.dumps(errors, ensure_ascii=False)
        
        for record_id, errors in validation_errors.items():
            if record_id in df.index:
                df.loc[record_id, "validation_errors"] = json.dumps(errors, ensure_ascii=False)
        
        return df
    
    def get_tracker(self) -> ErrorTracker:
        """Получить трекер ошибок."""
        return self.tracker


def create_error_tracker(entity_type: str) -> ErrorTracker:
    """Создать трекер ошибок для типа сущности."""
    return ErrorTracker(entity_type)


def create_dataframe_error_tracker(entity_type: str) -> DataFrameErrorTracker:
    """Создать трекер ошибок для DataFrame."""
    return DataFrameErrorTracker(entity_type)


def add_error_columns_to_dataframe(
    df: pd.DataFrame,
    entity_type: str,
    extraction_errors: dict[str, dict[str, str]] | None = None,
    validation_errors: dict[str, list[str]] | None = None
) -> pd.DataFrame:
    """Добавить колонки ошибок к DataFrame."""
    tracker = create_dataframe_error_tracker(entity_type)
    
    # Добавить ошибки извлечения
    if extraction_errors:
        for source, errors in extraction_errors.items():
            df = tracker.add_extraction_errors_column(df, source, errors)
    
    # Добавить ошибки валидации
    if validation_errors:
        df = tracker.add_validation_errors_column(df, validation_errors)
    
    # Добавить глобальные колонки ошибок
    df = tracker.add_global_error_columns(df)
    
    return df


def extract_errors_from_dataframe(df: pd.DataFrame) -> dict[str, Any]:
    """Извлечь ошибки из DataFrame."""
    errors = {
        "extraction_errors": {},
        "validation_errors": {},
        "extraction_status": {}
    }
    
    # Извлечь ошибки извлечения
    if "extraction_errors" in df.columns:
        for idx, error_json in df["extraction_errors"].items():
            if pd.notna(error_json):
                try:
                    errors["extraction_errors"][str(idx)] = json.loads(error_json)
                except (json.JSONDecodeError, TypeError):
                    pass
    
    # Извлечь ошибки валидации
    if "validation_errors" in df.columns:
        for idx, error_json in df["validation_errors"].items():
            if pd.notna(error_json):
                try:
                    errors["validation_errors"][str(idx)] = json.loads(error_json)
                except (json.JSONDecodeError, TypeError):
                    pass
    
    # Извлечь статус извлечения
    if "extraction_status" in df.columns:
        status_counts = df["extraction_status"].value_counts().to_dict()
        errors["extraction_status"] = {k: int(v) for k, v in status_counts.items()}
    
    return errors
