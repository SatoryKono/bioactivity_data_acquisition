from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence


@dataclass(frozen=True, slots=True)
class ColumnMapping:
    """Mapping between target column name and source payload fields."""

    column: str
    source_fields: Sequence[str]

    def __post_init__(self) -> None:
        if not self.column:
            raise ValueError("column name must be provided for ColumnMapping")
        if not self.source_fields:
            raise ValueError("source_fields must contain at least one key")


def extract_items(payload: Any) -> Iterable[Mapping[str, Any]]:
    """Yield mapping entries from common ChEMBL API payload shapes."""

    if isinstance(payload, Mapping):
        results = payload.get("results")
        if isinstance(results, list):
            for item in results:
                if isinstance(item, Mapping):
                    yield item
        elif payload:
            yield payload
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, Mapping):
                yield item


def _coalesce_item_fields(item: Mapping[str, Any], fields: Sequence[str]) -> Any:
    fallback = None
    for field in fields:
        candidate = item.get(field)
        if candidate:
            return candidate
        if fallback is None:
            fallback = candidate
    return fallback


def build_records_from_payload(payload: Any, mappings: Sequence[ColumnMapping]) -> list[dict[str, Any]]:
    """Create row dictionaries based on declared column mappings."""

    records: list[dict[str, Any]] = []
    for item in extract_items(payload):
        record: dict[str, Any] = {}
        for mapping in mappings:
            record[mapping.column] = _coalesce_item_fields(item, mapping.source_fields)
        records.append(record)
    return records


__all__ = ["ColumnMapping", "extract_items", "build_records_from_payload"]
