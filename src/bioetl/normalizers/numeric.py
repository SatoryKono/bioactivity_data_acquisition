"""Числовые и булевые нормализаторы."""

import math
from typing import Any

from bioetl.normalizers.base import BaseNormalizer
from bioetl.normalizers.constants import BOOLEAN_FALSE, BOOLEAN_TRUE
from bioetl.normalizers.helpers import _is_na


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
            return bool(value)
        text = str(value).strip().lower()
        if text in BOOLEAN_TRUE:
            return True
        if text in BOOLEAN_FALSE:
            return False
        return default

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
            return bool(value)
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
            return True
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
            return bool(value)
        text = str(value).strip().lower()
        if text in BOOLEAN_TRUE:
            return True
        if text in BOOLEAN_FALSE:
            return False
        return default
