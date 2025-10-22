"""Унифицированная логика обработки пустых значений для всех таблиц."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def is_empty_value(value: Any) -> bool:
    """Проверяет, является ли значение пустым.
    
    Args:
        value: Значение для проверки
        
    Returns:
        bool: True если значение пустое, False иначе
    """
    if value is None:
        return True
    
    # Проверяем на pd.isna только для скалярных значений
    try:
        if pd.isna(value):
            return True
    except (ValueError, TypeError):
        # Если pd.isna не может обработать тип (например, список), продолжаем
        pass
    
    if isinstance(value, str) and value.strip() == "":
        return True
    if isinstance(value, (list, tuple)) and len(value) == 0:
        return True
    if isinstance(value, dict) and len(value) == 0:
        return True
    return False


def normalize_string_field(value: Any) -> str | None:
    """Нормализует строковое поле.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        str | None: Нормализованная строка или None если значение пустое
    """
    if is_empty_value(value):
        return pd.NA
    
    try:
        if pd.isna(value):
            return pd.NA
    except (ValueError, TypeError):
        # Handle arrays and other non-scalar values
        pass
    
    str_value = str(value).strip()
    return str_value if str_value else pd.NA


def normalize_numeric_field(value: Any) -> float | None:
    """Нормализует числовое поле.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        float | None: Нормализованное число или None если значение пустое
    """
    if is_empty_value(value):
        return pd.NA
    
    try:
        if pd.isna(value):
            return pd.NA
    except (ValueError, TypeError):
        # Handle arrays and other non-scalar values
        pass
    
    try:
        return float(value)
    except (ValueError, TypeError):
        return pd.NA


def normalize_boolean_field(value: Any) -> bool | None:
    """Нормализует булево поле.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        bool | None: Нормализованное булево значение или None если значение пустое
    """
    if is_empty_value(value):
        return pd.NA
    
    try:
        if pd.isna(value):
            return pd.NA
    except (ValueError, TypeError):
        # Handle arrays and other non-scalar values
        pass
    
    if isinstance(value, bool):
        return value
    
    str_value = str(value).lower().strip()
    if str_value in ("true", "1", "yes", "y", "t"):
        return True
    elif str_value in ("false", "0", "no", "n", "f"):
        return False
    
    return pd.NA


def normalize_list_field(value: Any) -> list[str] | None:
    """Нормализует поле-список.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        list[str] | None: Нормализованный список или None если значение пустое
    """
    if is_empty_value(value):
        return pd.NA
    
    try:
        if pd.isna(value):
            return pd.NA
    except (ValueError, TypeError):
        # Handle arrays and other non-scalar values
        pass
    
    if isinstance(value, list):
        # Filter out empty values and normalize strings
        normalized_items = []
        for item in value:
            if not is_empty_value(item):
                str_item = str(item).strip()
                if str_item:
                    normalized_items.append(str_item)
        return normalized_items if normalized_items else pd.NA
    
    if isinstance(value, str):
        # Try to parse as JSON list or split by delimiters
        try:
            import json
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return normalize_list_field(parsed)
        except (json.JSONDecodeError, TypeError):
            pass
        
        # Split by common delimiters
        items = [item.strip() for item in value.replace(";", ",").split(",") if item.strip()]
        unique_items = list(set(items))
        return unique_items if unique_items else pd.NA
    
    return pd.NA


def normalize_dict_field(value: Any) -> dict[str, Any] | str | None:
    """Нормализует поле-словарь.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        dict[str, Any] | str | None: Нормализованный словарь или строка, или None если значение пустое
    """
    if is_empty_value(value):
        return pd.NA
    
    try:
        if pd.isna(value):
            return pd.NA
    except (ValueError, TypeError):
        # Handle arrays and other non-scalar values
        pass
    
    if isinstance(value, dict):
        return value
    
    if isinstance(value, str):
        try:
            import json
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            # If not JSON, return as string
            return value
    
    return str(value) if value is not None else pd.NA


def create_default_columns(column_definitions: dict[str, Any]) -> dict[str, Any]:
    """Создает словарь колонок по умолчанию с pd.NA значениями.
    
    Args:
        column_definitions: Словарь с определениями колонок
        
    Returns:
        dict[str, Any]: Словарь с колонками по умолчанию
    """
    default_columns = {}
    for column, default_value in column_definitions.items():
        if default_value is None:
            default_columns[column] = pd.NA
        else:
            default_columns[column] = default_value
    return default_columns


def normalize_dataframe_empty_values(df: pd.DataFrame, logger: logging.Logger | None = None) -> pd.DataFrame:
    """Нормализует пустые значения в DataFrame.
    
    Args:
        df: DataFrame для нормализации
        logger: Логгер для записи информации
        
    Returns:
        pd.DataFrame: DataFrame с нормализованными пустыми значениями
    """
    if df.empty:
        return df.copy()
    
    df_normalized = df.copy()
    
    if logger is not None:
        logger.info(f"Normalizing empty values in {len(df.columns)} columns, {len(df)} rows")
    
    for column in df_normalized.columns:
        if str(df_normalized[column].dtypes) == 'object':  # String columns
            # Replace None with pd.NA
            df_normalized[column] = df_normalized[column].replace([None], pd.NA)
            
            # Normalize empty strings to pd.NA
            df_normalized[column] = df_normalized[column].replace("", pd.NA)
            
            # Normalize 'nan', 'none', 'null' strings to pd.NA
            df_normalized[column] = df_normalized[column].replace(['nan', 'none', 'null'], pd.NA)
            
        elif pd.api.types.is_numeric_dtype(df_normalized[column]):
            # Numeric data - replace NaN with pd.NA for consistency
            import numpy as np
            df_normalized[column] = df_normalized[column].replace([np.nan], pd.NA)
            
        elif pd.api.types.is_bool_dtype(df_normalized[column]):
            # Boolean data - keep as is, but ensure consistency
            pass
    
    if logger is not None:
        logger.info("Empty values normalization completed")
    
    return df_normalized


def fill_required_fields(df: pd.DataFrame, required_fields: dict[str, Any], logger: logging.Logger | None = None) -> pd.DataFrame:
    """Заполняет обязательные поля значениями по умолчанию.
    
    Args:
        df: DataFrame для заполнения
        required_fields: Словарь с обязательными полями и их значениями по умолчанию
        logger: Логгер для записи информации
        
    Returns:
        pd.DataFrame: DataFrame с заполненными обязательными полями
    """
    df_filled = df.copy()
    
    for field, default_value in required_fields.items():
        if field in df_filled.columns:
            null_count = df_filled[field].isnull().sum()
            if null_count > 0:
                if logger is not None:
                    logger.warning(f"Filling {null_count} null values in {field} with default: {default_value}")
                df_filled[field] = df_filled[field].fillna(default_value)
    
    return df_filled
