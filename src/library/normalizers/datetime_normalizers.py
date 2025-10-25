"""
Нормализаторы для данных даты и времени.
"""

import datetime
import logging
from typing import Any

from .base import is_empty_value, register_normalizer, safe_normalize

logger = logging.getLogger(__name__)


@safe_normalize
def normalize_datetime_iso8601(value: Any) -> str | None:
    """Преобразует значение к ISO 8601 формату в UTC.

    Args:
        value: Значение для нормализации

    Returns:
        Дата в ISO 8601 формате или None
    """
    if is_empty_value(value):
        return None

    # Если уже строка в правильном формате
    if isinstance(value, str):
        try:
            # Пробуем распарсить и переформатировать
            dt = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, TypeError):
            pass

    # Если datetime объект
    if isinstance(value, datetime.datetime):
        # Убеждаемся что в UTC
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        else:
            value = value.astimezone(datetime.timezone.utc)
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Если date объект
    if isinstance(value, datetime.date):
        dt = datetime.datetime.combine(value, datetime.time.min)
        dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    return None


@safe_normalize
def normalize_datetime_validate(value: Any) -> str | None:
    """Проверяет валидность даты и возвращает в ISO 8601 формате.

    Args:
        value: Значение для нормализации

    Returns:
        Валидная дата в ISO 8601 формате или None
    """
    if is_empty_value(value):
        return None

    try:
        # Пробуем различные форматы
        if isinstance(value, str):
            # ISO 8601
            try:
                dt = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                pass

            # YYYY-MM-DD
            try:
                dt = datetime.datetime.strptime(value, "%Y-%m-%d")
                dt = dt.replace(tzinfo=datetime.timezone.utc)
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                pass

            # DD/MM/YYYY
            try:
                dt = datetime.datetime.strptime(value, "%d/%m/%Y")
                dt = dt.replace(tzinfo=datetime.timezone.utc)
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                pass

        # Если уже datetime объект
        if isinstance(value, datetime.datetime):
            return normalize_datetime_iso8601(value)

        # Если date объект
        if isinstance(value, datetime.date):
            return normalize_datetime_iso8601(value)

    except Exception as e:
        logger.debug(f"Failed to normalize datetime: {e}")

    return None


def normalize_datetime_precision(value: Any, precision: str = "seconds") -> str | None:
    """Обрезает дату до нужной точности.

    Args:
        value: Значение для нормализации
        precision: Точность ('seconds', 'minutes', 'hours', 'days')

    Returns:
        Дата с заданной точностью в ISO 8601 формате или None
    """
    if is_empty_value(value):
        return None

    # Сначала нормализуем к ISO 8601
    iso_date = normalize_datetime_iso8601(value)
    if iso_date is None:
        return None

    try:
        dt = datetime.datetime.fromisoformat(iso_date.replace("Z", "+00:00"))

        if precision == "days":
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        elif precision == "hours":
            dt = dt.replace(minute=0, second=0, microsecond=0)
        elif precision == "minutes":
            dt = dt.replace(second=0, microsecond=0)
        elif precision == "seconds":
            dt = dt.replace(microsecond=0)

        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    except Exception:
        return None


@safe_normalize
def normalize_date_only(value: Any) -> str | None:
    """Нормализует только дату (без времени).

    Args:
        value: Значение для нормализации

    Returns:
        Дата в формате YYYY-MM-DD или None
    """
    if is_empty_value(value):
        return None

    try:
        if isinstance(value, str):
            # Пробуем различные форматы даты
            for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"]:
                try:
                    dt = datetime.datetime.strptime(value, fmt)
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue

        # Если datetime объект
        if isinstance(value, datetime.datetime):
            return value.strftime("%Y-%m-%d")

        # Если date объект
        if isinstance(value, datetime.date):
            return value.strftime("%Y-%m-%d")

    except Exception as e:
        logger.debug(f"Failed to normalize datetime: {e}")

    return None


# Регистрация всех нормализаторов
register_normalizer("normalize_datetime_iso8601", normalize_datetime_iso8601)
register_normalizer("normalize_datetime_validate", normalize_datetime_validate)
register_normalizer("normalize_datetime_precision", normalize_datetime_precision)
register_normalizer("normalize_date_only", normalize_date_only)
