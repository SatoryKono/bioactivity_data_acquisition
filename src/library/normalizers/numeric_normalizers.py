"""
Нормализаторы для числовых данных.
"""

from typing import Any

from .base import is_empty_value, register_normalizer, safe_normalize


@safe_normalize
def normalize_int(value: Any) -> int | None:
    """Приводит значение к целому числу.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Целое число или None
    """
    if is_empty_value(value):
        return None
    
    if isinstance(value, int):
        return value
    
    if isinstance(value, float):
        # Проверяем на NaN
        if value != value:  # NaN check
            return None
        return int(value)
    
    if isinstance(value, str):
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    return None


@safe_normalize
def normalize_float(value: Any) -> float | None:
    """Приводит значение к числу с плавающей точкой.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Число с плавающей точкой или None
    """
    if is_empty_value(value):
        return None
    
    if isinstance(value, (int, float)):
        # Проверяем на NaN и inf
        if value != value or abs(value) == float('inf'):
            return None
        return float(value)
    
    if isinstance(value, str):
        try:
            result = float(value)
            # Проверяем на NaN и inf
            if result != result or abs(result) == float('inf'):
                return None
            return result
        except (ValueError, TypeError):
            return None
    
    return None


@safe_normalize
def normalize_int_positive(value: Any) -> int | None:
    """Приводит значение к положительному целому числу.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Положительное целое число или None
    """
    result = normalize_int(value)
    if result is not None and result > 0:
        return result
    return None


def normalize_int_range(value: Any, min_val: int | None = None, max_val: int | None = None) -> int | None:
    """Приводит значение к целому числу в заданном диапазоне.
    
    Args:
        value: Значение для нормализации
        min_val: Минимальное значение
        max_val: Максимальное значение
        
    Returns:
        Целое число в диапазоне или None
    """
    result = normalize_int(value)
    if result is None:
        return None
    
    if min_val is not None and result < min_val:
        return None
    
    if max_val is not None and result > max_val:
        return None
    
    return result


def normalize_float_precision(value: Any, precision: int = 6) -> float | None:
    """Приводит значение к числу с плавающей точкой с заданной точностью.
    
    Args:
        value: Значение для нормализации
        precision: Количество знаков после запятой
        
    Returns:
        Число с плавающей точкой с заданной точностью или None
    """
    result = normalize_float(value)
    if result is None:
        return None
    
    return round(result, precision)


# Специализированные функции для диапазонов
@safe_normalize
def normalize_year(value: Any) -> int | None:
    """Нормализует год (диапазон 1800-текущий год).
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Год в диапазоне 1800-текущий или None
    """
    import datetime
    current_year = datetime.datetime.now().year
    return normalize_int_range(value, 1800, current_year)


@safe_normalize
def normalize_month(value: Any) -> int | None:
    """Нормализует месяц (диапазон 1-12).
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Месяц в диапазоне 1-12 или None
    """
    return normalize_int_range(value, 1, 12)


@safe_normalize
def normalize_day(value: Any) -> int | None:
    """Нормализует день (диапазон 1-31).
    
    Args:
        value: Значение для нормализации
        
    Returns:
        День в диапазоне 1-31 или None
    """
    return normalize_int_range(value, 1, 31)


@safe_normalize
def normalize_molecular_weight(value: Any) -> float | None:
    """Нормализует молекулярную массу (диапазон 50-2000).
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Молекулярная масса в диапазоне 50-2000 или None
    """
    result = normalize_float(value)
    if result is None:
        return None
    
    if 50.0 <= result <= 2000.0:
        return round(result, 6)
    
    return None


@safe_normalize
def normalize_pchembl_range(value: Any) -> float | None:
    """Нормализует pChEMBL значение (диапазон 0-14).
    
    Args:
        value: Значение для нормализации
        
    Returns:
        pChEMBL значение в диапазоне 0-14 или None
    """
    result = normalize_float(value)
    if result is None:
        return None
    
    if 0.0 <= result <= 14.0:
        return round(result, 3)
    
    return None


# Регистрация всех нормализаторов
register_normalizer("normalize_int", normalize_int)
register_normalizer("normalize_float", normalize_float)
register_normalizer("normalize_int_positive", normalize_int_positive)
register_normalizer("normalize_int_range", normalize_int_range)
register_normalizer("normalize_float_precision", normalize_float_precision)
register_normalizer("normalize_year", normalize_year)
register_normalizer("normalize_month", normalize_month)
register_normalizer("normalize_day", normalize_day)
register_normalizer("normalize_molecular_weight", normalize_molecular_weight)
register_normalizer("normalize_pchembl_range", normalize_pchembl_range)
