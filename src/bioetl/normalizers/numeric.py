"""Числовые и булевые нормализаторы."""

import math
from typing import Any

from bioetl.normalizers.base import BaseNormalizer
from bioetl.normalizers.helpers import is_na, coerce_bool


class NumericNormalizer(BaseNormalizer):
    """Нормализатор числовых значений."""

    def normalize(self, value: Any, **_: Any) -> Any:
        """Основная нормализация - возвращает float."""
        return self.normalize_float(value)

    def validate(self, value: Any) -> bool:
        """Проверяет, можно ли нормализовать значение в число."""
        if is_na(value):
            return True
        try:
            float(str(value).strip())
            return True
        except (TypeError, ValueError):
            return False

    def normalize_int(self, value: Any) -> int | None:
        """Преобразует значение в целое число."""
        if is_na(value):
            return None
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return None

    def normalize_float(self, value: Any) -> float | None:
        """Преобразует значение в float."""
        if is_na(value):
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
        coerced = coerce_bool(value, default=default, allow_na=False)
        if coerced is None:
            return default
        return coerced

class BooleanNormalizer(BaseNormalizer):
    """Нормализатор булевых значений."""

    def normalize(self, value: Any, **_: Any) -> bool | None:
        """Нормализует булево значение, возвращая None для NA."""
        return coerce_bool(value, allow_na=True)

    def validate(self, value: Any) -> bool:
        """Проверяет, можно ли нормализовать значение в bool."""
        if is_na(value):
            return True
        return coerce_bool(value, allow_na=True) is not None

    def normalize_with_default(self, value: Any, default: bool = False) -> bool:
        """Нормализует булево значение с дефолтом для NA."""
        coerced = coerce_bool(value, default=default, allow_na=False)
        if coerced is None:
            return default
        return coerced
