"""
Нормализаторы для единиц измерения.
"""

from typing import Any

from .base import safe_normalize, register_normalizer, is_empty_value, ensure_string


@safe_normalize
def normalize_units(value: Any) -> str | None:
    """Нормализует единицы измерения.
    
    - strip(): удаление пробелов
    - lowercase(): приведение к нижнему регистру
    - map_units(): маппинг на стандартизованные единицы
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованные единицы измерения или None
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
    
    # Приводим к нижнему регистру
    text = text.lower()
    
    # Маппинг единиц на стандартизованные
    unit_mapping = {
        # Концентрации
        'nm': 'nm',
        'nanomolar': 'nm',
        'nanomol/l': 'nm',
        'nanomol/liter': 'nm',
        'nmol/l': 'nm',
        'nmol/liter': 'nm',
        
        'um': 'μm',
        'μm': 'μm',
        'micromolar': 'μm',
        'micromol/l': 'μm',
        'micromol/liter': 'μm',
        'μmol/l': 'μm',
        'μmol/liter': 'μm',
        'umol/l': 'μm',
        'umol/liter': 'μm',
        
        'mm': 'mm',
        'millimolar': 'mm',
        'millimol/l': 'mm',
        'millimol/liter': 'mm',
        'mmol/l': 'mm',
        'mmol/liter': 'mm',
        
        'm': 'm',
        'molar': 'm',
        'mol/l': 'm',
        'mol/liter': 'm',
        
        # Массы
        'ng/ml': 'ng/ml',
        'nanogram/ml': 'ng/ml',
        'nanogram/milliliter': 'ng/ml',
        
        'ug/ml': 'μg/ml',
        'μg/ml': 'μg/ml',
        'microgram/ml': 'μg/ml',
        'microgram/milliliter': 'μg/ml',
        
        'mg/ml': 'mg/ml',
        'milligram/ml': 'mg/ml',
        'milligram/milliliter': 'mg/ml',
        
        'g/ml': 'g/ml',
        'gram/ml': 'g/ml',
        'gram/milliliter': 'g/ml',
        
        # Проценты
        '%': '%',
        'percent': '%',
        'pct': '%',
        
        # Другие единицы
        'ic50': 'ic50',
        'ec50': 'ec50',
        'ki': 'ki',
        'kd': 'kd',
        'ic90': 'ic90',
        'ec90': 'ec90',
    }
    
    # Ищем точное совпадение
    if text in unit_mapping:
        return unit_mapping[text]
    
    # Ищем частичное совпадение (удаляем пробелы и дефисы)
    normalized_text = text.replace(' ', '').replace('-', '')
    for key, value in unit_mapping.items():
        if key.replace(' ', '').replace('-', '') == normalized_text:
            return value
    
    # Если не найдено, возвращаем исходное значение в нижнем регистре
    return text


@safe_normalize
def normalize_pchembl(value: Any) -> float | None:
    """Нормализует pChEMBL значение.
    
    - to_float64(): приведение к float64
    - range_0_14(): проверка диапазона [0-14]
    - round_precision(): округление до 3 знаков
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованное pChEMBL значение или None
    """
    if is_empty_value(value):
        return None
    
    # Приводим к числу
    try:
        if isinstance(value, (int, float)):
            pchembl_val = float(value)
        elif isinstance(value, str):
            pchembl_val = float(value)
        else:
            return None
    except (ValueError, TypeError):
        return None
    
    # Проверяем диапазон [0-14]
    if not (0.0 <= pchembl_val <= 14.0):
        return None
    
    # Округляем до 3 знаков
    return round(pchembl_val, 3)


@safe_normalize
def normalize_activity_value(value: Any) -> float | None:
    """Нормализует значение активности.
    
    - to_float64(): приведение к float64
    - check_positive(): проверка > 0
    - round_precision(): округление до 6 знаков
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованное значение активности или None
    """
    if is_empty_value(value):
        return None
    
    # Приводим к числу
    try:
        if isinstance(value, (int, float)):
            activity_val = float(value)
        elif isinstance(value, str):
            activity_val = float(value)
        else:
            return None
    except (ValueError, TypeError):
        return None
    
    # Проверяем что положительное
    if activity_val <= 0:
        return None
    
    # Округляем до 6 знаков
    return round(activity_val, 6)


def normalize_concentration(value: Any, unit: str | None = None) -> float | None:
    """Нормализует концентрацию с учетом единиц.
    
    Args:
        value: Значение концентрации
        unit: Единица измерения
        
    Returns:
        Нормализованная концентрация в моль/л или None
    """
    if is_empty_value(value):
        return None
    
    # Приводим к числу
    try:
        if isinstance(value, (int, float)):
            conc_val = float(value)
        elif isinstance(value, str):
            conc_val = float(value)
        else:
            return None
    except (ValueError, TypeError):
        return None
    
    # Проверяем что положительное
    if conc_val <= 0:
        return None
    
    # Конвертируем в моль/л если указана единица
    if unit:
        unit_lower = unit.lower()
        
        # Множители для конвертации в моль/л
        conversion_factors = {
            'nm': 1e-9,
            'μm': 1e-6,
            'mm': 1e-3,
            'm': 1.0,
        }
        
        if unit_lower in conversion_factors:
            conc_val *= conversion_factors[unit_lower]
    
    # Округляем до 6 знаков
    return round(conc_val, 6)


@safe_normalize
def normalize_percentage(value: Any) -> Optional[float]:
    """Нормализует процентное значение.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованное процентное значение (0-100) или None
    """
    if is_empty_value(value):
        return None
    
    # Приводим к числу
    try:
        if isinstance(value, (int, float)):
            pct_val = float(value)
        elif isinstance(value, str):
            # Удаляем символ %
            clean_value = value.replace('%', '').strip()
            pct_val = float(clean_value)
        else:
            return None
    except (ValueError, TypeError):
        return None
    
    # Проверяем диапазон [0-100]
    if not (0.0 <= pct_val <= 100.0):
        return None
    
    # Округляем до 2 знаков
    return round(pct_val, 2)


# Регистрация всех нормализаторов
register_normalizer("normalize_units", normalize_units)
register_normalizer("normalize_pchembl", normalize_pchembl)
register_normalizer("normalize_activity_value", normalize_activity_value)
register_normalizer("normalize_concentration", normalize_concentration)
register_normalizer("normalize_percentage", normalize_percentage)
