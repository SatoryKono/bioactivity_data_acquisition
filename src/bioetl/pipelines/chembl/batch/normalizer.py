"""Shared normalization utilities for Chembl batch pipelines."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any


class CommonNormalizer:
    """A minimal, extensible normalizer working on batches of dicts."""

    def normalize_batch(self, records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        normalized = [self.normalize_record(dict(record)) for record in records]
        return self.strip_strings(self.normalize_dates(normalized))

    def normalize_record(self, record: Mapping[str, Any]) -> dict[str, Any]:
        return dict(record)

    def normalize_dates(self, records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        def _convert(value: Any) -> Any:
            if isinstance(value, datetime):
                return value.isoformat()
            return value

        result: list[dict[str, Any]] = []
        for record in records:
            normalized = {k: _convert(v) for k, v in record.items()}
            result.append(normalized)
        return result

    def strip_strings(self, records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        cleaned: list[dict[str, Any]] = []
        for record in records:
            cleaned.append({k: v.strip() if isinstance(v, str) else v for k, v in record.items()})
        return cleaned

    def coerce_types(self, records: Iterable[Mapping[str, Any]], schema: Mapping[str, type]) -> list[dict[str, Any]]:
        coerced: list[dict[str, Any]] = []
        for record in records:
            updated = dict(record)
            for field_name, target_type in schema.items():
                value = updated.get(field_name)
                if value is None:
                    continue
                try:
                    updated[field_name] = target_type(value)
                except (TypeError, ValueError):
                    updated[field_name] = value
            coerced.append(updated)
        return coerced

    def drop_duplicates(self, records: Iterable[Mapping[str, Any]], key_fields: Iterable[str]) -> list[dict[str, Any]]:
        seen: set[tuple[Any, ...]] = set()
        unique: list[dict[str, Any]] = []
        for record in records:
            key = tuple(record.get(field) for field in key_fields)
            if key in seen:
                continue
            seen.add(key)
            unique.append(dict(record))
        return unique


__all__ = ["CommonNormalizer"]
