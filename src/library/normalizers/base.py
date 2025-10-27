"""
Базовые классы и утилиты для системы нормализации данных.
"""

import logging
from collections.abc import Callable
from typing import Any

from library.common.exceptions import NormalizationError

logger = logging.getLogger(__name__)

# Реестр всех доступных нормализаторов
_NORMALIZER_REGISTRY: dict[str, Callable[[Any], Any]] = {}


def register_normalizer(name: str, func: Callable[[Any], Any]) -> None:
    """Регистрирует функцию нормализации в реестре.

    Args:
        name: Имя функции нормализации
        func: Функция нормализации
    """
    _NORMALIZER_REGISTRY[name] = func
    logger.debug(f"Registered normalizer: {name}")


def get_normalizer(name: str) -> Callable[[Any], Any]:
    """Получает функцию нормализации по имени.

    Args:
        name: Имя функции нормализации

    Returns:
        Функция нормализации

    Raises:
        NormalizationError: Если функция не найдена
    """
    if name not in _NORMALIZER_REGISTRY:
        raise NormalizationError(f"Normalizer '{name}' not found in registry")
    return _NORMALIZER_REGISTRY[name]


def list_available_normalizers() -> list[str]:
    """Возвращает список всех доступных нормализаторов."""
    return list(_NORMALIZER_REGISTRY.keys())


def safe_normalize(func: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Декоратор для безопасной нормализации с обработкой исключений.

    Args:
        func: Функция нормализации

    Returns:
        Обернутая функция с обработкой исключений
    """

    def wrapper(value: Any) -> Any:
        try:
            return func(value)
        except Exception as e:
            logger.warning(f"Normalization failed for value '{value}' with function '{func.__name__}': {e}")
            return None

    return wrapper


def is_empty_value(value: Any) -> bool:
    """Проверяет, является ли значение пустым.

    Args:
        value: Значение для проверки

    Returns:
        True если значение пустое, False иначе
    """
    if value is None:
        return True

    if isinstance(value, str):
        return value.strip() == ""

    try:
        import pandas as pd

        if pd.isna(value):
            return True
    except ImportError:
        pass

    return False


def ensure_string(value: Any) -> str | None:
    """Приводит значение к строке, если это возможно.

    Args:
        value: Значение для приведения

    Returns:
        Строковое представление или None
    """
    if value is None:
        return None

    if isinstance(value, str):
        return value

    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.decode("utf-8", errors="ignore")

    return str(value)


def ensure_numeric(value: Any) -> float | None:
    """Приводит значение к числу, если это возможно.

    Args:
        value: Значение для приведения

    Returns:
        Числовое значение или None
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None

    return None
