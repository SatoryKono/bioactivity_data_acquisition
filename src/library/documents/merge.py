"""Модуль для объединения данных документов из разных источников."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def normalize_doi(doi: str | None) -> str:
    """Нормализовать DOI строку.
    
    Args:
        doi: DOI строка
        
    Returns:
        Нормализованная DOI строка
    """
    if not doi or pd.isna(doi):
        return ""
    
    doi = str(doi).strip()
    if not doi:
        return ""
    
    # Удаляем префиксы
    for prefix in ("doi:", "https://doi.org/", "http://doi.org/", "doi.org/"):
        if doi.lower().startswith(prefix):
            doi = doi[len(prefix):].strip()
    
    return doi


def resolve_conflicts(base_value: Any, source_value: Any, priority: str = "base") -> Any:
    """Разрешить конфликт между значениями из разных источников.
    
    Args:
        base_value: Значение из базового источника
        source_value: Значение из дополнительного источника
        priority: Приоритет ("base" или "source")
        
    Returns:
        Выбранное значение
    """
    if priority == "base":
        return base_value if base_value is not None and str(base_value).strip() else source_value
    else:
        return source_value if source_value is not None and str(source_value).strip() else base_value


def merge_metadata_records(*records: dict, source_priorities: dict | None = None) -> dict:
    """Объединить несколько записей метаданных.
    
    Args:
        *records: Словари с метаданными
        source_priorities: Приоритеты источников
        
    Returns:
        Объединённая запись
    """
    if source_priorities is None:
        source_priorities = {}
    
    merged = {}
    
    # Собираем все поля
    all_fields = set()
    for record in records:
        all_fields.update(record.keys())
    
    # Объединяем поля
    for field in all_fields:
        values = []
        for record in records:
            if field in record and record[field] is not None:
                values.append(record[field])
        
        if values:
            # Выбираем первое непустое значение
            merged[field] = next((v for v in values if v is not None and str(v).strip()), None)
        else:
            merged[field] = None
    
    # Нормализуем DOI
    if "doi" in merged:
        merged["doi"] = normalize_doi(merged["doi"])
        merged["doi_normalised"] = merged["doi"]
    
    return merged


def merge_source_data(base_df: pd.DataFrame, source_df: pd.DataFrame, source_name: str, join_key: str) -> pd.DataFrame:
    """Объединить данные из источника с базовыми данными.
    
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
    
    # Диагностическое логирование
    logger.debug(f"Merging {source_name}: base_df columns={list(base_df.columns)}, source_df columns={list(source_df.columns)}")
    logger.debug(f"Join key '{join_key}' present in base: {join_key in base_df.columns}, in source: {join_key in source_df.columns}")
    
    try:
        # Сбрасываем индексы для избежания проблем с дублированными метками
        base_df = base_df.reset_index(drop=True)
        source_df = source_df.reset_index(drop=True)
        
        # Нормализуем join key в source_df
        if isinstance(source_df[join_key], pd.Series):
            source_df[join_key] = source_df[join_key].astype(str).str.strip()
        else:
            logger.warning(f"Join key '{join_key}' in source data is not a Series, skipping normalization")
        
        # Фильтруем пустые ключи
        if isinstance(source_df[join_key], pd.Series):
            source_df = source_df[source_df[join_key].notna() & (source_df[join_key] != "")]
        else:
            logger.warning(f"Join key '{join_key}' in source data is not a Series, skipping filtering")
        
        if source_df.empty:
            logger.warning(f"No valid records to merge from {source_name} after filtering")
            return base_df
        
        # Нормализуем join key в base_df
        if join_key in base_df.columns:
            base_df[join_key] = base_df[join_key].astype(str).str.strip()
        
        # Простое объединение - добавляем колонки из source_df в base_df
        result_df = base_df.copy()
        
        # Добавляем новые колонки из source_df
        for col in source_df.columns:
            if col != join_key and col not in result_df.columns:
                result_df[col] = ""
        
        # Статистика для диагностики
        successful_merges = 0
        failed_merges = 0
        
        # Заполняем данные по индексу
        for idx in result_df.index:
            key_value = result_df.loc[idx, join_key]
            # Преобразуем key_value в строку для безопасного сравнения
            if pd.isna(key_value):
                key_value = ""
            else:
                key_value = str(key_value).strip()
            
            # Безопасное сравнение с обработкой NaN значений
            if key_value:
                try:
                    # Нормализуем join_key в source_df для безопасного сравнения
                    source_df_normalized = source_df.copy()
                    if join_key in source_df_normalized.columns:
                        source_df_normalized[join_key] = source_df_normalized[join_key].astype(str).str.strip()
                        
                        # Безопасное сравнение - используем .query для избежания проблем с boolean indexing
                        try:
                            # Фильтруем строки с совпадающим ключом
                            matching_rows = source_df_normalized[source_df_normalized[join_key] == key_value]
                            if len(matching_rows) > 0:
                                source_row = source_df.loc[matching_rows.index]
                            else:
                                source_row = pd.DataFrame()
                        except Exception as mask_error:
                            logger.warning(f"Error in mask comparison for key '{key_value}': {mask_error}")
                            source_row = pd.DataFrame()
                    else:
                        logger.warning(f"Join key '{join_key}' not found in source data")
                        source_row = pd.DataFrame()
                except Exception as e:
                    logger.warning(f"Error during merge for key '{key_value}': {e}")
                    source_row = pd.DataFrame()
            else:
                source_row = pd.DataFrame()
            
            # Проверяем количество найденных строк
            if len(source_row) > 0:
                for col in source_df.columns:
                    if col != join_key:
                        value = source_row.iloc[0][col]
                        # Обрабатываем NaN значения для boolean колонок
                        if pd.isna(value) and result_df[col].dtype == 'bool':
                            result_df.loc[idx, col] = False
                        else:
                            result_df.loc[idx, col] = value
                successful_merges += 1
            else:
                # Если join_key не найден, добавляем error поле для диагностики
                error_column = f"{source_name}_error"
                if error_column not in result_df.columns:
                    result_df[error_column] = ""
                result_df.loc[idx, error_column] = f"No matching {join_key} found"
                failed_merges += 1
        
        logger.info(f"Merge statistics for {source_name}: {successful_merges} successful, {failed_merges} failed out of {len(result_df)} records")
        return result_df
        
    except Exception as e:
        logger.error(f"Failed to merge data from {source_name}: {e}")
        # Добавляем error колонку для всех записей при критической ошибке
        error_column = f"{source_name}_error"
        if error_column not in base_df.columns:
            base_df[error_column] = ""
        base_df[error_column] = f"Merge failed: {str(e)}"
        return base_df


def compute_publication_date(df: pd.DataFrame) -> pd.DataFrame:
    """Вычислить единую дату публикации из различных полей дат.
    
    Args:
        df: DataFrame с данными документов
        
    Returns:
        DataFrame с добавленным полем publication_date
    """
    df = df.copy()
    
    # Список полей для поиска года, месяца, дня
    year_fields = ["pubmed_year", "crossref_year", "openalex_year", "chembl_year"]
    month_fields = ["pubmed_month", "crossref_month", "openalex_month", "chembl_month"]
    day_fields = ["pubmed_day", "crossref_day", "openalex_day", "chembl_day"]
    
    publication_dates = []
    
    for _, row in df.iterrows():
        year = None
        month = None
        day = None
        
        # Ищем год
        for field in year_fields:
            if field in row and pd.notna(row[field]) and str(row[field]).strip():
                try:
                    year = int(float(row[field]))
                    if 1900 <= year <= 2030:  # Разумные границы
                        break
                except (ValueError, TypeError):
                    continue
        
        # Ищем месяц
        for field in month_fields:
            if field in row and pd.notna(row[field]) and str(row[field]).strip():
                try:
                    month = int(float(row[field]))
                    if 1 <= month <= 12:
                        break
                except (ValueError, TypeError):
                    continue
        
        # Ищем день
        for field in day_fields:
            if field in row and pd.notna(row[field]) and str(row[field]).strip():
                try:
                    day = int(float(row[field]))
                    if 1 <= day <= 31:
                        break
                except (ValueError, TypeError):
                    continue
        
        # Формируем дату
        if year:
            if month and day:
                date_str = f"{year:04d}-{month:02d}-{day:02d}"
            elif month:
                date_str = f"{year:04d}-{month:02d}-01"
            else:
                date_str = f"{year:04d}-01-01"
        else:
            date_str = ""
        
        publication_dates.append(date_str)
    
    df["publication_date"] = publication_dates
    return df


def add_document_sortorder(df: pd.DataFrame) -> pd.DataFrame:
    """Добавить поле document_sortorder для детерминированного порядка.
    
    Args:
        df: DataFrame с данными документов
        
    Returns:
        DataFrame с добавленным полем document_sortorder
    """
    df = df.copy()
    
    if df.empty:
        df["document_sortorder"] = []
        return df
    
    # Сортируем по document_chembl_id для детерминированного порядка
    if "document_chembl_id" in df.columns:
        df = df.sort_values("document_chembl_id").reset_index(drop=True)
    
    df["document_sortorder"] = [str(i) for i in range(len(df))]
    
    return df


def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """Преобразовать типы данных в DataFrame для соответствия схеме.
    
    Args:
        df: DataFrame с данными документов
        
    Returns:
        DataFrame с правильными типами данных
    """
    df = df.copy()
    
    # Преобразуем числовые поля в строки
    string_fields = [
        "pubmed_year", "pubmed_month", "pubmed_day",
        "crossref_year", "crossref_month", "crossref_day",
        "openalex_year", "openalex_month", "openalex_day",
        "chembl_month", "chembl_day",
        "document_sortorder"
    ]
    
    for field in string_fields:
        if field in df.columns:
            df[field] = df[field].astype(str)
    
    # Преобразуем year и chembl_year в float64
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors='coerce').astype('float64')
    
    if "chembl_year" in df.columns:
        df["chembl_year"] = pd.to_numeric(df["chembl_year"], errors='coerce').astype('float64')
    
    return df