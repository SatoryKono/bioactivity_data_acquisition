"""Утилиты для добавления системных метаданных в пайплайны."""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from typing import Any

import pandas as pd

from library.clients.chembl import ChEMBLClient

logger = logging.getLogger(__name__)


def add_system_metadata_fields(
    df: pd.DataFrame,
    config: dict[str, Any],
    chembl_client: ChEMBLClient | None = None,
) -> pd.DataFrame:
    """Добавить системные метаданные поля в DataFrame.

    Добавляет следующие поля:
    - index: порядковый номер записи (0-based)
    - pipeline_version: версия пайплайна из конфигурации
    - source_system: система-источник
    - chembl_release: версия ChEMBL из API
    - extracted_at: время извлечения данных
    - hash_row: SHA256 хеш всей строки
    - hash_business_key: SHA256 хеш бизнес-ключа

    Args:
        df: DataFrame для добавления метаданных
        config: Конфигурация пайплайна
        chembl_client: ChEMBL клиент для получения версии (опционально)

    Returns:
        DataFrame с добавленными метаданными полями
    """
    if df.empty:
        logger.warning("DataFrame is empty, returning empty DataFrame with metadata columns")
        return df.copy()

    df_with_metadata = df.copy()

    # Удаляем существующие метаданные колонки, если они есть, чтобы избежать дубликатов
    metadata_columns = ["index", "pipeline_version", "source_system", "chembl_release", "extracted_at", "hash_row", "hash_business_key"]
    for col in metadata_columns:
        if col in df_with_metadata.columns:
            df_with_metadata = df_with_metadata.drop(columns=[col])

    # Index - порядковый номер записи (0-based)
    df_with_metadata["index"] = range(len(df))

    # Pipeline version - из конфигурации
    pipeline_version = config.get("pipeline", {}).get("version", "2.0.0")
    df_with_metadata["pipeline_version"] = pipeline_version

    # Source system - из конфигурации или по умолчанию
    source_system = config.get("pipeline", {}).get("source_system", "chembl")
    df_with_metadata["source_system"] = source_system

    # ChEMBL release - динамически из API или из конфига
    chembl_release = None
    if chembl_client:
        try:
            # ChEMBLClient не имеет метода get_chembl_status()
            # Используем fallback на конфигурацию
            logger.debug("ChEMBLClient does not have get_chembl_status method, using config fallback")
        except Exception as e:
            logger.warning(f"Failed to get ChEMBL release from API: {e}")

    # Fallback на конфигурацию если API недоступен
    if not chembl_release:
        chembl_release = config.get("sources", {}).get("chembl", {}).get("release")
        if chembl_release:
            logger.info(f"Using ChEMBL release from config: {chembl_release}")
        else:
            logger.warning("ChEMBL release not available from API or config")

    df_with_metadata["chembl_release"] = chembl_release

    # Extracted at - текущее время UTC
    df_with_metadata["extracted_at"] = datetime.utcnow().isoformat() + "Z"

    # Hash row - SHA256 хеш всей строки (исключая hash поля)
    df_with_metadata["hash_row"] = df_with_metadata.apply(lambda row: _calculate_row_hash(row, exclude_hash_fields=True), axis=1)

    # Hash business key - SHA256 хеш бизнес-ключа
    # Определяем бизнес-ключ на основе типа пайплайна
    business_key_fields = _get_business_key_fields(df_with_metadata)
    if business_key_fields:
        df_with_metadata["hash_business_key"] = df_with_metadata[business_key_fields].apply(lambda row: _calculate_business_key_hash(row), axis=1)
    else:
        # Fallback на hash_row если не можем определить бизнес-ключ
        df_with_metadata["hash_business_key"] = df_with_metadata["hash_row"]
        logger.warning("Could not determine business key fields, using hash_row as fallback")

    logger.info(f"Added system metadata fields to {len(df_with_metadata)} records")
    return df_with_metadata


def _calculate_row_hash(row: pd.Series, exclude_hash_fields: bool = True) -> str:
    """Вычислить SHA256 хеш строки.

    Args:
        row: Строка DataFrame
        exclude_hash_fields: Исключить hash поля из расчета

    Returns:
        SHA256 хеш в hex формате
    """
    # Исключаем hash поля и системные поля из расчета
    exclude_fields = ["hash_row", "hash_business_key", "extracted_at"]
    if exclude_hash_fields:
        exclude_fields.extend(["hash_row", "hash_business_key"])

    # Создаем строку для хеширования
    hash_data = []
    for col, value in row.items():
        if col not in exclude_fields:
            # Приводим к строке и нормализуем
            # Обрабатываем пустые массивы и NaN значения
            try:
                if hasattr(value, "size") and value.size == 0:
                    # Пустой массив (numpy array, pandas Series, etc.)
                    str_value = ""
                elif pd.isna(value):
                    str_value = ""
                else:
                    str_value = str(value)
            except (ValueError, TypeError):
                # Fallback для проблемных типов данных
                str_value = str(value) if value is not None else ""
            hash_data.append(f"{col}:{str_value}")

    # Сортируем для детерминизма
    hash_data.sort()
    hash_string = "|".join(hash_data)

    return hashlib.sha256(hash_string.encode("utf-8")).hexdigest()


def _calculate_business_key_hash(row: pd.Series) -> str:
    """Вычислить SHA256 хеш бизнес-ключа.

    Args:
        row: Строка DataFrame с полями бизнес-ключа

    Returns:
        SHA256 хеш в hex формате
    """
    # Создаем строку для хеширования из полей бизнес-ключа
    hash_data = []
    for col, value in row.items():
        str_value = str(value) if pd.notna(value) else ""
        hash_data.append(f"{col}:{str_value}")

    # Сортируем для детерминизма
    hash_data.sort()
    hash_string = "|".join(hash_data)

    return hashlib.sha256(hash_string.encode("utf-8")).hexdigest()


def _get_business_key_fields(df: pd.DataFrame) -> list[str]:
    """Определить поля бизнес-ключа на основе доступных колонок.

    Args:
        df: DataFrame для анализа

    Returns:
        Список полей бизнес-ключа
    """
    # Определяем бизнес-ключ на основе типа пайплайна
    if "activity_chembl_id" in df.columns:
        return ["activity_chembl_id"]
    elif "document_chembl_id" in df.columns:
        return ["document_chembl_id"]
    elif "assay_chembl_id" in df.columns:
        return ["assay_chembl_id"]
    elif "molecule_chembl_id" in df.columns:
        return ["molecule_chembl_id"]
    elif "target_chembl_id" in df.columns:
        return ["target_chembl_id"]
    else:
        # Fallback - ищем любые ChEMBL ID поля
        chembl_id_cols = [col for col in df.columns if col.endswith("_chembl_id")]
        if chembl_id_cols:
            return chembl_id_cols[:1]  # Берем первый найденный
        return []
