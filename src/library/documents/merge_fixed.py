"""Утилиты для объединения данных документов по образцу референсного проекта."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def merge_series_prefer_left(left: pd.Series[Any], right: pd.Series[Any]) -> pd.Series[Any]:
    """Объединить серии, предпочитая значения из левой серии.
    
    Функция заполняет пропуски в левой серии значениями из правой серии,
    сохраняя существующие значения в левой серии.
    
    Args:
        left: Основная серия
        right: Дополнительная серия для заполнения пропусков
        
    Returns:
        Объединённая серия
    """
    if left.empty and right.empty:
        return left.copy()

    original_index = left.index

    left_work = left.copy()
    right_work = right.copy()

    # Проверяем необходимость выравнивания индексов
    requires_alignment = (
        not original_index.equals(right.index)
        or not left_work.index.is_unique
        or not right_work.index.is_unique
    )

    if requires_alignment:
        # Выравниваем индексы
        if not left_work.index.equals(right_work.index):
            right_work = right_work.reindex(left_work.index)

    result = left_work.copy()
    missing_mask = result.isna()
    if missing_mask.any():
        result.loc[missing_mask] = right_work.loc[missing_mask]

    return result


def merge_source_data_fixed(base_df: pd.DataFrame, source_df: pd.DataFrame, source_name: str, join_key: str) -> pd.DataFrame:
    """Объединить данные из источника с базовыми данными (исправленная версия).
    
    Args:
        base_df: Базовый DataFrame
        source_df: DataFrame с данными из источника
        source_name: Название источника
        join_key: Ключ для объединения
        
    Returns:
        Объединённый DataFrame
    """
    if source_df.empty:
        logger.warning(f"No data to merge from {source_name}")
        return base_df

    logger.info(f"Merging {len(source_df)} records from {source_name} using key '{join_key}'")

    try:
        # Проверяем наличие ключа объединения
        if join_key not in base_df.columns:
            logger.warning(f"Join key '{join_key}' not found in base data")
            return base_df
            
        if join_key not in source_df.columns:
            logger.warning(f"Join key '{join_key}' not found in source data")
            return base_df

        # Нормализуем ключи объединения
        base_df = base_df.copy()
        source_df = source_df.copy()
        
        base_df[join_key] = pd.to_numeric(base_df[join_key], errors="coerce").astype("Int64").astype("string").fillna("")
        source_df[join_key] = pd.to_numeric(source_df[join_key], errors="coerce").astype("Int64").astype("string").fillna("")

        # Устанавливаем индекс для объединения
        result = base_df.set_index(join_key, drop=False)
        
        # Объединяем данные по индексу
        for column in source_df.columns:
            if column == join_key:
                continue
                
            # Добавляем колонку если её нет
            if column not in result.columns:
                result[column] = pd.Series([pd.NA] * len(result), index=result.index, dtype="object")
            
            # Объединяем данные, предпочитая существующие значения
            source_series = source_df.set_index(join_key)[column]
            result[column] = merge_series_prefer_left(result[column], source_series)

        # Возвращаем к обычному индексу
        result = result.reset_index(drop=True)
        
        logger.info(f"Successfully merged {len(source_df)} records from {source_name}")
        return result

    except Exception as e:
        logger.error(f"Failed to merge data from {source_name}: {e}")
        # Добавляем error колонку для всех записей при критической ошибке
        error_column = f"{source_name}_error"
        if error_column not in base_df.columns:
            base_df[error_column] = ""
        base_df[error_column] = f"Merge failed: {str(e)}"
        return base_df
