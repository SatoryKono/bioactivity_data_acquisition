"""Validation helpers for Chembl batch pipelines."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Sequence

from .base import ValidationError


class CommonValidator:
    """Validator that can be shared across Chembl entities."""

    def __init__(self, required_fields: Sequence[str] | None = None) -> None:
        self.required_fields = list(required_fields or [])

    def ensure_ids_found(self, requested_ids: Sequence[Any], returned_ids: Sequence[Any | None]) -> list[ValidationError]:
        requested_set = set(requested_ids)
        returned_set = {item for item in returned_ids if item is not None}
        missing = requested_set - returned_set
        return [ValidationError(record_id=item, message="Requested id not found") for item in sorted(missing)]

    def validate_records(self, records: Iterable[Mapping[str, Any]]) -> tuple[list[ValidationError], list[Mapping[str, Any]]]:
        errors: list[ValidationError] = []
        valid: list[Mapping[str, Any]] = []
        for record in records:
            record_errors = self._validate_required_fields(record)
            if record_errors:
                errors.extend(record_errors)
                continue
            valid.append(record)
        return errors, valid

    def _validate_required_fields(self, record: Mapping[str, Any]) -> list[ValidationError]:
        errors: list[ValidationError] = []
        for field_name in self.required_fields:
            if field_name not in record:
                errors.append(ValidationError(record_id=record.get(field_name), message=f"Missing field: {field_name}"))
            elif record.get(field_name) is None:
                errors.append(
                    ValidationError(record_id=record.get(field_name), message=f"Field {field_name} must not be null")
                )
        return errors

    def validate_ranges(
        self, records: Iterable[Mapping[str, Any]], field: str, min_value: float, max_value: float
    ) -> list[ValidationError]:
        errors: list[ValidationError] = []
        for record in records:
            value = record.get(field)
            if value is None:
                continue
            try:
                float_value = float(value)
            except (TypeError, ValueError):
                errors.append(ValidationError(record_id=record.get(field), message=f"Field {field} is not numeric"))
                continue
            if not (min_value <= float_value <= max_value):
                errors.append(
                    ValidationError(
                        record_id=record.get(field),
                        message=f"Field {field} must be between {min_value} and {max_value}",
                    )
                )
        return errors


__all__ = ["CommonValidator"]
