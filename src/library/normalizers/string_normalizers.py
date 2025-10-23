"""
Нормализаторы для строковых данных.
"""

import re
import unicodedata
from typing import Any, Optional

from .base import safe_normalize, register_normalizer, is_empty_value, ensure_string


@safe_normalize
def normalize_string_strip(value: Any) -> str | None:
    """Удаляет ведущие и завершающие пробелы.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Строка без пробелов или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    stripped = text.strip()
    return stripped if stripped else None


@safe_normalize
def normalize_string_upper(value: Any) -> str | None:
    """Приводит строку к верхнему регистру.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Строка в верхнем регистре или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    return text.upper()


@safe_normalize
def normalize_string_lower(value: Any) -> str | None:
    """Приводит строку к нижнему регистру.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Строка в нижнем регистре или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    return text.lower()


@safe_normalize
def normalize_string_titlecase(value: Any) -> str | None:
    """Приводит строку к title case.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Строка в title case или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    return text.title()


@safe_normalize
def normalize_string_nfc(value: Any) -> str | None:
    """Приводит строку к Unicode NFC нормализации.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованная строка или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    return unicodedata.normalize('NFC', text)


@safe_normalize
def normalize_string_whitespace(value: Any) -> str | None:
    """Нормализует внутренние пробелы (заменяет множественные пробелы на один).
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Строка с нормализованными пробелами или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    # Заменяем множественные пробелы на один
    normalized = re.sub(r'\s+', ' ', text)
    return normalized.strip() if normalized else None


@safe_normalize
def normalize_empty_to_null(value: Any) -> str | None:
    """Преобразует пустые строки в None.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Строка или None если пустая
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    return text if text.strip() else None


# Регистрация всех нормализаторов
register_normalizer("normalize_string_strip", normalize_string_strip)
register_normalizer("normalize_string_upper", normalize_string_upper)
register_normalizer("normalize_string_lower", normalize_string_lower)
register_normalizer("normalize_string_titlecase", normalize_string_titlecase)
register_normalizer("normalize_string_nfc", normalize_string_nfc)
register_normalizer("normalize_string_whitespace", normalize_string_whitespace)
register_normalizer("normalize_empty_to_null", normalize_empty_to_null)
