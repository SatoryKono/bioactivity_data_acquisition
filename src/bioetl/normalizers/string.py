"""String normalizer."""

import re
import unicodedata
from typing import Any

from bioetl.normalizers.base import BaseNormalizer


class StringNormalizer(BaseNormalizer):
    """Нормализатор строк."""

    def __init__(self):
        self.normalizations = [
            self.strip,
            self.nfc,
            self.whitespace,
        ]

    def normalize(self, value: str | None, **_: Any) -> str | None:
        """Нормализует строку."""
        if value is None or not isinstance(value, str):
            return None

        result = value
        for func in self.normalizations:
            result = func(result)

        return result if result else None

    def strip(self, s: str) -> str:
        """Удаляет пробелы в начале и конце."""
        return s.strip()

    def nfc(self, s: str) -> str:
        """Приводит к Unicode NFC."""
        return unicodedata.normalize("NFC", s)

    def whitespace(self, s: str) -> str:
        """Нормализует множественные пробелы."""
        return re.sub(r"\s+", " ", s)

    def validate(self, value: Any) -> bool:
        """Проверяет, что значение является строкой."""
        return isinstance(value, str)
