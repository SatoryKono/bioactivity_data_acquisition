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

    def validate(self, value: Any) -> bool:
        """Проверяет корректность идентификатора."""
        if not isinstance(value, str):
            return False

        for pattern in self.PATTERNS.values():
            if re.match(pattern, value):
                return True

        return False

