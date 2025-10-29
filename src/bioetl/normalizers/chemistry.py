"""Chemistry-focused normalizers used across ETL pipelines."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any

from bioetl.normalizers.base import BaseNormalizer

NA_STRINGS = {"", "na", "n/a", "none", "null", "nan"}
RELATION_ALIASES = {
    "==": "=",
    "=": "=",
    "≡": "=",
    "<": "<",
    "≤": "<=",
    "⩽": "<=",
    "⩾": ">=",
    "≥": ">=",
    ">": ">",
    "~": "~",
}
UNIT_SYNONYMS = {
    "nm": "nM",
    "nanomolar": "nM",
    "μm": "µM",
    "µm": "µM",
    "um": "µM",
    "μmolar": "µM",
    "µmolar": "µM",
    "mum": "µM",
    "μmol/l": "µmol/L",
    "µmol/l": "µmol/L",
    "umol/l": "µmol/L",
    "mmol/l": "mmol/L",
    "mol/l": "mol/L",
    "μg/ml": "µg/mL",
    "μg/mL": "µg/mL",
    "µg/ml": "µg/mL",
    "µg/mL": "µg/mL",
    "ug/ml": "µg/mL",
    "ug/mL": "µg/mL",
    "ng/ml": "ng/mL",
    "pg/ml": "pg/mL",
    "mg/ml": "mg/mL",
    "mg/l": "mg/L",
    "ug/l": "µg/L",
    "μg/l": "µg/L",
    "µg/l": "µg/L",
}
BOOLEAN_TRUE = {"true", "1", "yes", "y", "t"}
BOOLEAN_FALSE = {"false", "0", "no", "n", "f"}


def is_na(value: Any) -> bool:
    """Return ``True`` when the value should be treated as missing."""

    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str):
        candidate = value.strip()
        if candidate == "":
            return True
        return candidate.lower() in NA_STRINGS
    return False


def _canonicalize_whitespace(text: str) -> str:
    """Collapse consecutive whitespace characters into a single space."""

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


@dataclass(slots=True)
class ChemistryStringNormalizer(BaseNormalizer):
    """Normalize textual values with canonical whitespace and casing rules."""

    uppercase: bool = False
    title_case: bool = False
    max_length: int | None = None

    def normalize(self, value: Any) -> str | None:
        if is_na(value):
            return None

        text = _canonicalize_whitespace(str(value))
        if not text:
            return None

        if self.uppercase:
            text = text.upper()
        elif self.title_case:
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

        if self.max_length is not None:
            text = text[: self.max_length]

        return text or None

    def validate(self, value: Any) -> bool:
        return isinstance(value, str)


class ChemblIdNormalizer(ChemistryStringNormalizer):
    """Normalize ChEMBL identifiers to uppercase form."""

    pattern = re.compile(r"^CHEMBL\d+$")

    def __init__(self, *, strict: bool = False) -> None:
        super().__init__(uppercase=True)
        self.strict = strict

    def normalize(self, value: Any) -> str | None:
        normalized = super().normalize(value)
        if normalized is None:
            return None
        if self.strict and not self.pattern.fullmatch(normalized):
            return None
        return normalized

    def validate(self, value: Any) -> bool:
        if not isinstance(value, str):
            return False
        if self.strict:
            return bool(self.pattern.fullmatch(value.strip().upper()))
        return bool(value.strip())


class BaoIdNormalizer(ChemistryStringNormalizer):
    """Normalize BAO identifiers."""

    def __init__(self) -> None:
        super().__init__(uppercase=True)


@dataclass(slots=True)
class ChemistryUnitNormalizer(BaseNormalizer):
    """Normalize measurement units using a synonym map."""

    default: str | None = None

    def normalize(self, value: Any) -> str | None:
        if is_na(value):
            return self.default

        text = _canonicalize_whitespace(str(value))
        if not text:
            return self.default

        canonical = UNIT_SYNONYMS.get(text.lower())
        return canonical or text

    def validate(self, value: Any) -> bool:
        return isinstance(value, str)


@dataclass(slots=True)
class RelationNormalizer(BaseNormalizer):
    """Normalize relation operators to ASCII equivalents."""

    default: str = "="

    def normalize(self, value: Any) -> str:
        if is_na(value):
            return self.default
        relation = str(value).strip()
        if not relation:
            return self.default
        return RELATION_ALIASES.get(
            relation,
            RELATION_ALIASES.get(relation.lower(), self.default),
        )

    def validate(self, value: Any) -> bool:
        return isinstance(value, str)


class FloatNormalizer(BaseNormalizer):
    """Convert values to floating point numbers when possible."""

    def normalize(self, value: Any) -> float | None:
        if is_na(value):
            return None
        try:
            result = float(str(value).strip())
        except (TypeError, ValueError):
            return None
        if math.isnan(result):
            return None
        return result

    def validate(self, value: Any) -> bool:
        return isinstance(value, (int, float, str))


class IntNormalizer(BaseNormalizer):
    """Convert values to integers when possible."""

    def normalize(self, value: Any) -> int | None:
        if is_na(value):
            return None
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return None

    def validate(self, value: Any) -> bool:
        return isinstance(value, (int, float, str))


@dataclass(slots=True)
class BooleanNormalizer(BaseNormalizer):
    """Normalize boolean-like values to strict bools."""

    default: bool = False

    def normalize(self, value: Any) -> bool:
        if is_na(value):
            return self.default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if text in BOOLEAN_TRUE:
            return True
        if text in BOOLEAN_FALSE:
            return False
        return self.default

    def validate(self, value: Any) -> bool:
        return isinstance(value, (bool, int, float, str))


