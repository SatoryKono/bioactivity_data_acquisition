"""Identifier normalizer."""

import re
from typing import Any

from bioetl.normalizers.base import BaseNormalizer


class IdentifierNormalizer(BaseNormalizer):
    """Нормализатор идентификаторов."""

    PATTERNS = {
        "doi": r"^10\.\d+/.+$",
        "pmid": r"^\d+$",
        "chembl_id": r"^CHEMBL\d+$",
        "uniprot": r"^[A-Z0-9]{6,10}(-[0-9]+)?$",
        "pubchem_cid": r"^\d+$",
        "inchi_key": r"^[A-Z]{14}-[A-Z]{10}-[A-Z]$",
        "orcid": r"^\d{4}-\d{4}-\d{4}-\d{3}[0-9X]$",
        "openalex": r"^[A-Z]\d+$",
    }

    def normalize(self, value: str) -> str | None:
        """Нормализует идентификатор."""
        if not value or not isinstance(value, str):
            return None

        s = value.strip().upper()

        # ChEMBL ID
        if s.startswith("CHEMBL"):
            return s

        # DOI
        if s.startswith("10."):
            return s.lower()

        # PMID, PubChem CID
        if s.isdigit():
            return s

        # UniProt
        if re.match(self.PATTERNS["uniprot"], s):
            return s.upper()

        return s

    def normalize_orcid(self, value: str) -> str | None:
        """Нормализует ORCID ID."""
        if not value or not isinstance(value, str):
            return None

        # Убрать URL префикс
        orcid = value.replace("https://orcid.org/", "").replace("http://orcid.org/", "")
        orcid = orcid.strip()

        # Валидация формата
        if re.match(self.PATTERNS["orcid"], orcid):
            return orcid

        return None

    def normalize_openalex_id(self, value: str) -> str | None:
        """Извлекает короткий OpenAlex ID из URL."""
        if not value or not isinstance(value, str):
            return None

        # Извлечь ID из URL вида https://openalex.org/W123456789
        pattern = r"https://openalex\.org/([A-Z]\d+)"
        match = re.match(pattern, value)
        if match:
            return match.group(1)

        # Если уже короткий ID
        if re.match(self.PATTERNS["openalex"], value):
            return value

        return None

    def validate(self, value: Any) -> bool:
        """Проверяет корректность идентификатора."""
        if not isinstance(value, str):
            return False

        for pattern in self.PATTERNS.values():
            if re.match(pattern, value):
                return True

        return False

