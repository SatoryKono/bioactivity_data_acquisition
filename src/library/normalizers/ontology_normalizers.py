"""
Нормализаторы для онтологических данных (BAO, GO и др.).
"""

import re
from typing import Any

from .base import safe_normalize, register_normalizer, is_empty_value, ensure_string


@safe_normalize
def normalize_bao_id(value: Any) -> str | None:
    """Нормализует BAO ID.
    
    - strip(): удаление пробелов
    - uppercase(): приведение к верхнему регистру
    - validate_bao_pattern(): проверка формата ^BAO_\d+$
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованный BAO ID или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    # Удаляем пробелы
    text = text.strip()
    if not text:
        return None
    
    # Приводим к верхнему регистру
    text = text.upper()
    
    # Удаляем префиксы если есть
    if text.startswith('BAO:'):
        text = text[4:]
    elif text.startswith('BAO_'):
        pass  # Уже правильный формат
    elif text.startswith('BAO'):
        # Пробуем добавить подчеркивание
        if len(text) > 3 and text[3].isdigit():
            text = f"BAO_{text[3:]}"
        else:
            text = f"BAO_{text[3:]}"
    
    # Валидация формата BAO ID
    # Формат: ^BAO_\d+$
    bao_pattern = r'^BAO_\d+$'
    
    if not re.match(bao_pattern, text):
        return None
    
    return text


@safe_normalize
def normalize_bao_label(value: Any) -> str | None:
    """Нормализует BAO label.
    
    - strip(): удаление пробелов
    - titlecase(): приведение к title case
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованный BAO label или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    # Удаляем пробелы
    text = text.strip()
    if not text:
        return None
    
    # Приводим к title case
    text = text.title()
    
    return text


@safe_normalize
def normalize_go_id(value: Any) -> str | None:
    """Нормализует GO ID.
    
    - strip(): удаление пробелов
    - uppercase(): приведение к верхнему регистру
    - validate_go_pattern(): проверка формата ^GO:\d{7}$
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованный GO ID или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    # Удаляем пробелы
    text = text.strip()
    if not text:
        return None
    
    # Приводим к верхнему регистру
    text = text.upper()
    
    # Убеждаемся что есть префикс GO:
    if not text.startswith('GO:'):
        # Пробуем добавить префикс
        if text.startswith('GO'):
            text = f"GO:{text[2:]}"
        else:
            text = f"GO:{text}"
    
    # Валидация формата GO ID
    # Формат: ^GO:\d{7}$
    go_pattern = r'^GO:\d{7}$'
    
    if not re.match(go_pattern, text):
        return None
    
    return text


@safe_normalize
def normalize_ec_number(value: Any) -> str | None:
    """Нормализует EC номер.
    
    - strip(): удаление пробелов
    - validate_ec_pattern(): проверка формата EC номеров
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованный EC номер или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    # Удаляем пробелы
    text = text.strip()
    if not text:
        return None
    
    # Удаляем префикс EC если есть
    if text.upper().startswith('EC '):
        text = text[3:]
    elif text.upper().startswith('EC:'):
        text = text[3:]
    elif text.upper().startswith('EC'):
        text = text[2:]
    
    # Валидация формата EC номера
    # Формат: \d+\.\d+\.\d+\.\d+ или \d+\.\d+\.\d+\.- или \d+\.\d+\.-\.- или \d+\.-\.-\.-
    ec_pattern = r'^\d+\.(\d+\.(\d+\.(\d+|-)|-\.-)|-\.-\.-)$'
    
    if not re.match(ec_pattern, text):
        return None
    
    return text


@safe_normalize
def normalize_hgnc_id(value: Any) -> str | None:
    """Нормализует HGNC ID.
    
    - strip(): удаление пробелов
    - uppercase(): приведение к верхнему регистру
    - validate_hgnc_pattern(): проверка формата ^HGNC:\d+$
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованный HGNC ID или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    # Удаляем пробелы
    text = text.strip()
    if not text:
        return None
    
    # Приводим к верхнему регистру
    text = text.upper()
    
    # Убеждаемся что есть префикс HGNC:
    if not text.startswith('HGNC:'):
        # Пробуем добавить префикс
        if text.startswith('HGNC'):
            text = f"HGNC:{text[4:]}"
        else:
            text = f"HGNC:{text}"
    
    # Валидация формата HGNC ID
    # Формат: ^HGNC:\d+$
    hgnc_pattern = r'^HGNC:\d+$'
    
    if not re.match(hgnc_pattern, text):
        return None
    
    return text


# Регистрация всех нормализаторов
register_normalizer("normalize_bao_id", normalize_bao_id)
register_normalizer("normalize_bao_label", normalize_bao_label)
register_normalizer("normalize_go_id", normalize_go_id)
register_normalizer("normalize_ec_number", normalize_ec_number)
register_normalizer("normalize_hgnc_id", normalize_hgnc_id)
