"""Identifier normalizer."""

import re
from typing import Any

from bioetl.normalizers.base import BaseNormalizer


class IdentifierNormalizer(BaseNormalizer):
    """Нормализатор идентификаторов."""

    PATTERNS = {
        "doi": r"^10\.\d{4,}/.+$",
        "pmid": r"^\d+$",
        "chembl_id": r"^CHEMBL\d+$",
        "uniprot": r"^[A-Z0-9]{5}[0-9](?:-[0-9]+)?$",
        "pubchem_cid": r"^\d+$",
        "inchi_key": r"^[A-Z]{14}-[A-Z]{10}-[A-Z]$",
        "orcid": r"^\d{4}-\d{4}-\d{4}-\d{3}[0-9X]$",
        "openalex": r"^[WASICF]\d+$",
    }

    def normalize(self, value: str) -> str | None:
        """Нормализует идентификатор."""
        if not isinstance(value, str):
            return None

        text = value.strip()
        if not text:
            return None

        upper_text = text.upper()

        # ChEMBL ID
        if re.fullmatch(self.PATTERNS["chembl_id"], upper_text):
            return upper_text

        # DOI
        if re.fullmatch(self.PATTERNS["doi"], text, flags=re.IGNORECASE):
            return text.lower()

        # PMID, PubChem CID
        if re.fullmatch(self.PATTERNS["pmid"], text):
            return text

        # UniProt accession
        if re.fullmatch(self.PATTERNS["uniprot"], upper_text):
            return upper_text

        # InChI Key
        if re.fullmatch(self.PATTERNS["inchi_key"], upper_text):
            return upper_text

        return None

    def normalize_orcid(self, value: str) -> str | None:
        """Нормализует ORCID ID."""
        if not isinstance(value, str):
            return None

        # Убрать URL префикс
        orcid = value.replace("https://orcid.org/", "").replace("http://orcid.org/", "")
        orcid = orcid.strip().upper()

        # Валидация формата
        if re.match(self.PATTERNS["orcid"], orcid):
            return orcid

        return None

    def normalize_openalex_id(self, value: str) -> str | None:
        """Извлекает короткий OpenAlex ID из URL."""
        if not isinstance(value, str):
            return None

        candidate = value.strip()

        # Извлечь ID из URL вида https://openalex.org/W123456789
        pattern = r"https://openalex\.org/([A-Z]\d+)"
        match = re.match(pattern, candidate)
        if match:
            return match.group(1)

        # Если уже короткий ID
        if re.match(self.PATTERNS["openalex"], candidate):
            return candidate

        return None

    def validate(self, value: Any) -> bool:
        """Проверяет корректность идентификатора."""
        if not isinstance(value, str):
            return False

        text = value.strip()
        if not text:
            return False

        upper_text = text.upper()

        if re.fullmatch(self.PATTERNS["chembl_id"], upper_text):
            return True

        if re.fullmatch(self.PATTERNS["doi"], text, flags=re.IGNORECASE):
            return True

        if re.fullmatch(self.PATTERNS["pmid"], text):
            return True

        if re.fullmatch(self.PATTERNS["pubchem_cid"], text):
            return True

        if re.fullmatch(self.PATTERNS["uniprot"], upper_text):
            return True

        if re.fullmatch(self.PATTERNS["inchi_key"], upper_text):
            return True

        if re.fullmatch(self.PATTERNS["orcid"], text.upper()):
            return True

        if re.fullmatch(self.PATTERNS["openalex"], upper_text):
            return True

        return False

