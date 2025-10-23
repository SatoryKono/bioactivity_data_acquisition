"""
Нормализаторы для булевых данных.
"""

from typing import Any

from .base import is_empty_value, register_normalizer, safe_normalize


@safe_normalize
def normalize_boolean(value: Any) -> bool | None:
    """Преобразует значение в булево с маппингом строк/чисел.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Булево значение или None
    """
    if is_empty_value(value):
        return None
    
    # Если уже булево
    if isinstance(value, bool):
        return value
    
    # Если число
    if isinstance(value, (int, float)):
        return bool(value)
    
    # Если строка
    if isinstance(value, str):
        value_lower = value.lower().strip()
        
        # True значения
        if value_lower in ['true', 't', '1', 'yes', 'y', 'on', 'enabled', 'active']:
            return True
        
        # False значения
        if value_lower in ['false', 'f', '0', 'no', 'n', 'off', 'disabled', 'inactive']:
            return False
    
    return None


@safe_normalize
def normalize_boolean_strict(value: Any) -> bool | None:
    """Строгая проверка булевых значений.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Булево значение или None
    """
    if is_empty_value(value):
        return None
    
    # Если уже булево
    if isinstance(value, bool):
        return value
    
    # Если число
    if isinstance(value, (int, float)):
        if value == 0:
            return False
        elif value == 1:
            return True
        return None  # Другие числа не принимаем
    
    # Если строка - только строгие значения
    if isinstance(value, str):
        value_lower = value.lower().strip()
        
        if value_lower in ['true', '1']:
            return True
        elif value_lower in ['false', '0']:
            return False
    
    return None


def normalize_boolean_from_numeric(value: Any, threshold: float = 0) -> bool | None:
    """Преобразует числовое значение в булево по порогу.
    
    Args:
        value: Значение для нормализации
        threshold: Пороговое значение (по умолчанию 0)
        
    Returns:
        Булево значение или None
    """
    if is_empty_value(value):
        return None
    
    # Если уже булево
    if isinstance(value, bool):
        return value
    
    # Если число
    if isinstance(value, (int, float)):
        return value > threshold
    
    # Если строка - пробуем преобразовать в число
    if isinstance(value, str):
        try:
            num_value = float(value)
            return num_value > threshold
        except (ValueError, TypeError):
            pass
    
    return None


@safe_normalize
def normalize_boolean_preserve_none(value: Any) -> bool | None:
    """Преобразует значение в булево, сохраняя None как None.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Булево значение или None
    """
    if value is None:
        return None
    
    return normalize_boolean(value)


# Регистрация всех нормализаторов
register_normalizer("normalize_boolean", normalize_boolean)
register_normalizer("normalize_boolean_strict", normalize_boolean_strict)
register_normalizer("normalize_boolean_from_numeric", normalize_boolean_from_numeric)
register_normalizer("normalize_boolean_preserve_none", normalize_boolean_preserve_none)
