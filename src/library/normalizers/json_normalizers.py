"""
JSON нормализаторы для обработки JSON данных.

Предоставляет функции для нормализации и валидации JSON структур,
используемых в полях CHEMBL.DOCS.chembl_release и CHEMBL.SOURCE.data.
"""

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def normalize_json_keys(value: Any) -> str | None:
    """
    Нормализация ключей JSON объекта.

    Применяется к полю CHEMBL.DOCS.chembl_release для нормализации
    ключей JSON объекта в соответствии со спецификацией.

    Args:
        value: JSON объект (dict) или JSON строка

    Returns:
        str | None: Нормализованная JSON строка или None

    Example:
        >>> normalize_json_keys({"chembl_release": "CHEMBL_36"})
        '{"chembl_release":"CHEMBL_36"}'
    """
    if value is None:
        return None

    try:
        # Если это уже строка, попробуем распарсить
        if isinstance(value, str):
            if not value.strip():
                return None
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                # Если не JSON, возвращаем как есть
                return value
        elif isinstance(value, dict):
            parsed = value
        else:
            # Для других типов попробуем преобразовать в dict
            parsed = dict(value) if hasattr(value, "items") else {"value": str(value)}

        # Нормализация ключей: приведение к нижнему регистру и удаление пробелов
        normalized = {}
        for key, val in parsed.items():
            if isinstance(key, str):
                # Нормализация ключа
                normalized_key = key.strip().lower().replace(" ", "_")
                normalized[normalized_key] = val
            else:
                normalized[str(key)] = val

        # Сериализация в компактный JSON
        return json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))

    except Exception as e:
        logger.warning("Failed to normalize JSON keys: %s", e)
        # Возвращаем исходное значение как строку
        return str(value) if value is not None else None


def normalize_json_structure(value: Any) -> str | None:
    """
    Валидация и нормализация JSON структуры.

    Применяется к полю CHEMBL.SOURCE.data для валидации и нормализации
    JSON структуры согласно схеме SOURCE.

    Args:
        value: JSON объект (dict) или JSON строка

    Returns:
        str | None: Валидированная и нормализованная JSON строка или None

    Example:
        >>> normalize_json_structure({"src_id": 1, "src_description": "Test"})
        '{"src_id":1,"src_description":"Test"}'
    """
    if value is None:
        return None

    try:
        # Если это уже строка, попробуем распарсить
        if isinstance(value, str):
            if not value.strip():
                return None
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                logger.warning("Invalid JSON string: %s", value)
                return None
        elif isinstance(value, dict):
            parsed = value
        else:
            # Для других типов попробуем преобразовать в dict
            parsed = dict(value) if hasattr(value, "items") else {"value": str(value)}

        # Валидация структуры SOURCE
        validated = {}

        # Обязательные поля SOURCE
        source_fields = {"src_id": "src_id", "src_description": "src_description", "src_short_name": "src_short_name", "src_url": "src_url", "src_comment": "src_comment"}

        for field, key in source_fields.items():
            if key in parsed:
                val = parsed[key]
                # Валидация типов
                if field == "src_id" and val is not None:
                    try:
                        validated[field] = int(val)
                    except (ValueError, TypeError):
                        logger.warning("Invalid src_id type: %s", val)
                        validated[field] = None
                elif field == "src_url" and val is not None:
                    # Базовая валидация URL
                    if isinstance(val, str) and (val.startswith("http://") or val.startswith("https://")):
                        validated[field] = val.strip()
                    else:
                        logger.warning("Invalid URL format: %s", val)
                        validated[field] = None
                else:
                    # Строковые поля
                    if val is not None:
                        validated[field] = str(val).strip() if str(val).strip() else None
                    else:
                        validated[field] = None

        # Добавляем дополнительные поля если они есть
        for key, val in parsed.items():
            if key not in source_fields and key not in validated:
                if val is not None:
                    validated[key] = str(val).strip() if str(val).strip() else None

        # Сериализация в компактный JSON
        return json.dumps(validated, ensure_ascii=False, separators=(",", ":"))

    except Exception as e:
        logger.warning("Failed to normalize JSON structure: %s", e)
        # Возвращаем исходное значение как строку
        return str(value) if value is not None else None


def normalize_pmid(value: Any) -> str | None:
    """
    Нормализация PubMed ID.

    Применяется к полю CHEMBL.DOCS.pubmed_id для нормализации
    PubMed идентификатора.

    Args:
        value: PubMed ID (int, str) или None

    Returns:
        str | None: Нормализованный PMID или None

    Example:
        >>> normalize_pmid(15109676)
        '15109676'
        >>> normalize_pmid("15109676")
        '15109676'
    """
    if value is None:
        return None

    try:
        # Преобразуем в строку и убираем пробелы
        pmid_str = str(value).strip()

        # Проверяем что это число
        if not pmid_str.isdigit():
            logger.warning("Invalid PMID format: %s", value)
            return None

        # Проверяем разумный диапазон PMID
        pmid_int = int(pmid_str)
        if pmid_int < 1 or pmid_int > 999999999:
            logger.warning("PMID out of reasonable range: %s", value)
            return None

        return pmid_str

    except (ValueError, TypeError) as e:
        logger.warning("Failed to normalize PMID: %s", e)
        return None


def normalize_year(value: Any) -> int | None:
    """
    Нормализация года.

    Применяется к полю CHEMBL.DOCS.year для нормализации
    года публикации.

    Args:
        value: Год (int, str) или None

    Returns:
        int | None: Нормализованный год или None

    Example:
        >>> normalize_year(2004)
        2004
        >>> normalize_year("2004")
        2004
    """
    if value is None:
        return None

    try:
        # Преобразуем в int
        year = int(float(str(value).strip()))

        # Проверяем разумный диапазон
        if 1800 <= year <= 2100:
            return year
        else:
            logger.warning("Year out of reasonable range: %s", value)
            return None

    except (ValueError, TypeError) as e:
        logger.warning("Failed to normalize year: %s", e)
        return None


def normalize_month(value: Any) -> int | None:
    """
    Нормализация месяца.

    Применяется к полям месяца для нормализации
    месяца в диапазоне 1-12.

    Args:
        value: Месяц (int, str) или None

    Returns:
        int | None: Нормализованный месяц или None

    Example:
        >>> normalize_month(3)
        3
        >>> normalize_month("12")
        12
    """
    if value is None:
        return None

    try:
        # Преобразуем в int
        month = int(float(str(value).strip()))

        # Проверяем диапазон
        if 1 <= month <= 12:
            return month
        else:
            logger.warning("Month out of range 1-12: %s", value)
            return None

    except (ValueError, TypeError) as e:
        logger.warning("Failed to normalize month: %s", e)
        return None


def normalize_day(value: Any) -> int | None:
    """
    Нормализация дня.

    Применяется к полям дня для нормализации
    дня в диапазоне 1-31.

    Args:
        value: День (int, str) или None

    Returns:
        int | None: Нормализованный день или None

    Example:
        >>> normalize_day(15)
        15
        >>> normalize_day("31")
        31
    """
    if value is None:
        return None

    try:
        # Преобразуем в int
        day = int(float(str(value).strip()))

        # Проверяем диапазон
        if 1 <= day <= 31:
            return day
        else:
            logger.warning("Day out of range 1-31: %s", value)
            return None

    except (ValueError, TypeError) as e:
        logger.warning("Failed to normalize day: %s", e)
        return None
