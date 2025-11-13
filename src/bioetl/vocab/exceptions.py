"""Custom exceptions for vocabulary validation and lookup failures."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from bioetl.core.runtime.errors import BioETLError


@dataclass(frozen=True, slots=True)
class VocabularyViolation:
    """Structured detail about invalid values for a vocabulary binding."""

    column: str
    vocabulary_id: str
    invalid_values: tuple[str, ...]
    invalid_count: int


class VocabularyValidationError(BioETLError):
    """Raised when dataset values violate declared vocabulary bindings."""

    def __init__(self, *, violations: Iterable[VocabularyViolation]) -> None:
        violations_tuple = tuple(violations)
        if not violations_tuple:
            message = "Vocabulary validation error without violations payload."
            raise ValueError(message)

        details = [
            {
                "column": violation.column,
                "vocabulary_id": violation.vocabulary_id,
                "invalid_values": violation.invalid_values,
                "invalid_count": violation.invalid_count,
            }
            for violation in violations_tuple
        ]
        super().__init__("Values do not satisfy vocabulary bindings.")
        self.violations: tuple[VocabularyViolation, ...] = violations_tuple
        self.context: dict[str, object] = {"violations": details}

