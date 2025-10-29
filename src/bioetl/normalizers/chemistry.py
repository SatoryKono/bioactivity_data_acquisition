"""Chemistry normalizers used across pipelines."""

from __future__ import annotations

import math
import re
from typing import Any

from bioetl.core.logger import UnifiedLogger
from bioetl.normalizers.base import BaseNormalizer
from bioetl.normalizers.constants import NA_STRINGS
from bioetl.normalizers.numeric import NumericNormalizer

logger = UnifiedLogger.get(__name__)


def _is_na(value: Any) -> bool:
    """Return True if the provided value should be treated as NA."""

    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return True
        return stripped.lower() in NA_STRINGS
    return False


def _canonicalize_whitespace(text: str) -> str:
    """Collapse consecutive whitespace into single spaces."""

    return re.sub(r"\s+", " ", text.strip())


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

    def normalize(self, value: Any, **_: Any) -> Any:
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


class ChemistryStringNormalizer(BaseNormalizer):
    """Нормализует химические текстовые значения с параметрами."""

    def normalize(
        self,
        value: Any,
        *,
        uppercase: bool = False,
        title_case: bool = False,
        max_length: int | None = None,
        **_: Any,
    ) -> str | None:
        """Normalize textual values with canonical whitespace and casing."""

        if _is_na(value):
            return None

        text = _canonicalize_whitespace(str(value))
        if not text:
            return None

        if uppercase:
            text = text.upper()
        elif title_case:
            tokens = text.split(" ")
            normalized_tokens: list[str] = []
            for token in tokens:
                if token.isupper() and len(token) <= 4:
                    normalized_tokens.append(token)
                elif token.lower() in {"sp.", "spp.", "cf."}:
                    normalized_tokens.append(token.lower())
                else:
                    normalized_tokens.append(token.capitalize())
            text = " ".join(normalized_tokens)

        if max_length is not None:
            text = text[:max_length]
        return text or None

    def validate(self, value: Any) -> bool:
        """Проверяет, что значение можно нормализовать как строку."""

        return _is_na(value) or isinstance(value, str)


class ChemblIdNormalizer(BaseNormalizer):
    """Normalize ChEMBL identifiers to uppercase canonical form."""

    def __init__(self) -> None:
        self._string_normalizer = ChemistryStringNormalizer()

    def normalize(self, value: Any, **_: Any) -> str | None:
        if _is_na(value):
            return None

        return self._string_normalizer.normalize(value, uppercase=True)

    def validate(self, value: Any) -> bool:
        if _is_na(value):
            return True
        if not isinstance(value, str):
            return False
        return bool(value.strip())


class BaoIdNormalizer(BaseNormalizer):
    """Normalize BAO identifiers."""

    def __init__(self) -> None:
        self._string_normalizer = ChemistryStringNormalizer()
        self._pattern = re.compile(r"^BAO[:_]\d{7}$", re.IGNORECASE)

    def normalize(self, value: Any, **_: Any) -> str | None:
        if _is_na(value):
            return None

        text = self._string_normalizer.normalize(value, uppercase=True)
        if text is None:
            return None
        # accept BAO synonyms even if pattern doesn't match perfectly
        if self._pattern.match(text):
            return text.replace(":", "_") if ":" in text else text
        return text

    def validate(self, value: Any) -> bool:
        if _is_na(value):
            return True
        if not isinstance(value, str):
            return False
        return bool(self._pattern.match(value.strip()))


class TargetOrganismNormalizer(BaseNormalizer):
    """Normalize organism names to title case with canonical whitespace."""

    def __init__(self) -> None:
        self._string_normalizer = ChemistryStringNormalizer()

    def normalize(self, value: Any, **_: Any) -> str | None:
        return self._string_normalizer.normalize(value, title_case=True)

    def validate(self, value: Any) -> bool:
        return _is_na(value) or isinstance(value, str)


class NonNegativeFloatNormalizer(BaseNormalizer):
    """Convert values to non-negative floats with logging."""

    def __init__(self) -> None:
        self._numeric_normalizer = NumericNormalizer()

    def normalize(self, value: Any, *, column: str | None = None, **_: Any) -> float | None:
        result = self._numeric_normalizer.normalize(value)
        if result is None:
            return None
        if result < 0:
            logger.warning(
                "non_negative_float_sanitized",
                column=column,
                original_value=result,
                sanitized_value=None,
            )
            return None
        return result

    def validate(self, value: Any) -> bool:
        return self._numeric_normalizer.validate(value)


class LigandEfficiencyNormalizer(BaseNormalizer):
    """Normalize ligand efficiency payloads into numeric tuple."""

    def __init__(self) -> None:
        self._numeric_normalizer = NumericNormalizer()

    def normalize(
        self,
        value: Any,
        **_: Any,
    ) -> tuple[float | None, float | None, float | None, float | None]:
        if not isinstance(value, dict):
            return None, None, None, None

        bei = self._numeric_normalizer.normalize(value.get("bei"))
        sei = self._numeric_normalizer.normalize(value.get("sei"))
        le = self._numeric_normalizer.normalize(value.get("le"))
        lle = self._numeric_normalizer.normalize(value.get("lle"))
        return bei, sei, le, lle

    def validate(self, value: Any) -> bool:
        return value is None or isinstance(value, dict)

