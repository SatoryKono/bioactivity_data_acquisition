"""Chemistry normalizer."""

import re
from typing import Any

from bioetl.normalizers.base import BaseNormalizer


class ChemistryNormalizer(BaseNormalizer):
    """Нормализатор химических структур."""

    def normalize_smiles(self, value: str) -> str | None:
        """Нормализует SMILES."""
        if not value:
            return None

        s = value.strip()
        s = re.sub(r"\s+", " ", s)

        return s if s else None

    def normalize_inchi(self, value: str) -> str | None:
        """Нормализует InChI."""
        if not value:
            return None

        s = value.strip()

        if not s.startswith("InChI="):
            return None

        return s

    def normalize(self, value: Any) -> Any:
        """Нормализует химическую структуру."""
        if not isinstance(value, str):
            return None

        # Try InChI first
        if value.startswith("InChI="):
            return self.normalize_inchi(value)

        # Try SMILES
        return self.normalize_smiles(value)

    def validate(self, value: Any) -> bool:
        """Проверяет корректность химической структуры."""
        if not isinstance(value, str):
            return False

        # Check if it's a valid InChI or SMILES
        if value.startswith("InChI="):
            return self.normalize_inchi(value) is not None

        # Basic SMILES validation (non-empty string)
        return bool(value.strip())

