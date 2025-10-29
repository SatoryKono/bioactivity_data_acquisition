"""Числовые и булевые нормализаторы."""

import math
import re
from typing import Any

from bioetl.normalizers.base import BaseNormalizer
from bioetl.normalizers.constants import (
    BOOLEAN_FALSE,
    BOOLEAN_TRUE,
    NA_STRINGS,
    RELATION_ALIASES,
    UNIT_SYNONYMS,
)


def _is_na(value: Any) -> bool:
    """Проверяет, является ли значение NA/пустым."""
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str):
        if value.strip() == "":
            return True
        return value.strip().lower() in NA_STRINGS
    return False


def _canonicalize_whitespace(text: str) -> str:
    """Нормализует пробелы в строке."""
    return re.sub(r"\s+", " ", text.strip())


class NumericNormalizer(BaseNormalizer):
    """Нормализатор числовых значений."""

    def normalize(self, value: Any, **_: Any) -> Any:
        """Основная нормализация - возвращает float."""
        return self.normalize_float(value)

    def validate(self, value: Any) -> bool:
        """Проверяет, можно ли нормализовать значение в число."""
        if _is_na(value):
            return True
        try:
            float(str(value).strip())
            return True
        except (TypeError, ValueError):
            return False

    def normalize_int(self, value: Any) -> int | None:
        """Преобразует значение в целое число."""
        if _is_na(value):
            return None
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return None

    def normalize_float(self, value: Any) -> float | None:
        """Преобразует значение в float."""
        if _is_na(value):
            return None
        try:
            result = float(str(value).strip())
        except (TypeError, ValueError):
            return None
        if math.isnan(result):
            return None
        return result

    def normalize_bool(self, value: Any, *, default: bool = False) -> bool:
        """Нормализует булево значение."""
        if _is_na(value):
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and not isinstance(value, bool):
            return bool(value)
        if isinstance(value, float):
            if value.is_integer():
                return bool(int(value))
            return None
        text = str(value).strip().lower()
        if text in BOOLEAN_TRUE:
            return True
        if text in BOOLEAN_FALSE:
            return False
        return default

    def normalize_relation(self, value: Any, default: str = "=") -> str:
        """Нормализует знаки сравнения."""
        if _is_na(value):
            return default
        relation = str(value).strip()
        if relation == "":
            return default
        return RELATION_ALIASES.get(relation, RELATION_ALIASES.get(relation.lower(), default))

    def normalize_units(self, value: Any, default: str | None = None) -> str | None:
        """Нормализует единицы измерения."""
        if _is_na(value):
            return default
        text = _canonicalize_whitespace(str(value))
        key = text.lower()
        canonical = UNIT_SYNONYMS.get(key)
        if canonical is not None:
            return canonical
        return text


class BooleanNormalizer(BaseNormalizer):
    """Нормализатор булевых значений."""

    def normalize(self, value: Any, **_: Any) -> bool | None:
        """Нормализует булево значение, возвращая None для NA."""
        if _is_na(value):
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and not isinstance(value, bool):
            return bool(value)
        if isinstance(value, float):
            if value.is_integer():
                return bool(int(value))
            return None
        text = str(value).strip().lower()
        if text in BOOLEAN_TRUE:
            return True
        if text in BOOLEAN_FALSE:
            return False
        return None

    def validate(self, value: Any) -> bool:
        """Проверяет, можно ли нормализовать значение в bool."""
        if _is_na(value):
            return True
        if isinstance(value, bool):
            return True
        if isinstance(value, int) and not isinstance(value, bool):
            return True
        if isinstance(value, float):
            return value.is_integer()
        if isinstance(value, str):
            text = str(value).strip().lower()
            return text in BOOLEAN_TRUE or text in BOOLEAN_FALSE
        return False

    def normalize_with_default(self, value: Any, default: bool = False) -> bool:
        """Нормализует булево значение с дефолтом для NA."""
        if _is_na(value):
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and not isinstance(value, bool):
            return bool(value)
        if isinstance(value, float):
            if value.is_integer():
                return bool(int(value))
            return default
        text = str(value).strip().lower()
        if text in BOOLEAN_TRUE:
            return True
        if text in BOOLEAN_FALSE:
            return False
        return default
