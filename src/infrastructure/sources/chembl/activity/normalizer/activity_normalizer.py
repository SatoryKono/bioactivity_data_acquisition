"""Chembl activity normalizer implementing the shared contract."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from bioetl.base_classes import INormalizer
from infrastructure.schemas import get_schema

__all__ = ["ChemblActivityNormalizer"]

_DEFAULT_SCHEMA_IDENTIFIER = (
    "infrastructure.schemas.chembl_activity_schema.ActivitySchema"
)


class ChemblActivityNormalizer(INormalizer):
    """Normalize parsed ChEMBL activity records against the canonical schema."""

    def __init__(
        self, *, schema_identifier: str = _DEFAULT_SCHEMA_IDENTIFIER
    ) -> None:
        descriptor = get_schema(schema_identifier)
        self._schema_identifier = schema_identifier
        self._column_order: tuple[str, ...] = tuple(
            descriptor.schema.columns.keys()
        )

    def normalize(self, record: Mapping[str, Any] | None) -> dict[str, Any]:
        """Return a dictionary aligned to the configured schema columns."""

        payload = dict(record or {})
        normalized: dict[str, Any] = {}
        for column in self._column_order:
            normalized[column] = payload.get(column)
        return normalized

    @property
    def schema_identifier(self) -> str:
        """Expose the schema identifier used by the normalizer."""

        return self._schema_identifier
