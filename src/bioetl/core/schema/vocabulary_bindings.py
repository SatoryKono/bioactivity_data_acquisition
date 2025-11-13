"""Shared helpers for embedding vocabulary metadata in schema definitions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

VOCAB_METADATA_KEY = "vocabulary"


def _normalize_statuses(statuses: object) -> tuple[str, ...]:
    if statuses is None:
        return ()
    if isinstance(statuses, (str, bytes)):
        return (str(statuses),)
    if isinstance(statuses, Iterable):
        return tuple(str(status) for status in statuses)
    return ()


@dataclass(frozen=True, slots=True)
class SchemaVocabularyBinding:
    """Vocabulary metadata attached to a schema column."""

    column: str
    vocabulary_id: str
    allowed_statuses: tuple[str, ...] = ()
    required: bool = True

    def to_metadata(self) -> Mapping[str, object]:
        """Return a JSON-serializable mapping for schema-level metadata."""

        payload: dict[str, object] = {
            "column": self.column,
            "vocabulary_id": self.vocabulary_id,
        }
        if self.allowed_statuses:
            payload["allowed_statuses"] = self.allowed_statuses
        payload["required"] = self.required
        return payload

    @classmethod
    def from_column_metadata(
        cls,
        column_name: str,
        metadata: Mapping[str, object] | None,
    ) -> "SchemaVocabularyBinding | None":
        """Build a binding from column metadata when present."""

        if not metadata:
            return None
        payload = metadata.get(VOCAB_METADATA_KEY)
        if not isinstance(payload, Mapping):
            return None

        vocabulary_id = payload.get("id") or payload.get("vocabulary_id")
        if not isinstance(vocabulary_id, str) or not vocabulary_id.strip():
            return None

        allowed_statuses = _normalize_statuses(payload.get("allowed_statuses"))
        required_raw = payload.get("required", True)
        required = bool(required_raw)

        return cls(
            column=column_name,
            vocabulary_id=vocabulary_id.strip(),
            allowed_statuses=allowed_statuses,
            required=required,
        )

