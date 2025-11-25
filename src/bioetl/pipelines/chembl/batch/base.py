"""Base classes and data structures for Chembl batch pipelines."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol
import json

from .config import PipelineConfig


class ChemblDbClient(Protocol):
    """Protocol defining the minimal DB access required by the pipelines."""

    def fetch_by_ids(
        self, table: str, id_field: str, ids: Sequence[Any], *, include_related: bool = False
    ) -> Sequence[Mapping[str, Any]]:
        """Return full table rows for the given identifiers."""

    def save_records(
        self, table: str, records: Sequence[Mapping[str, Any]]
    ) -> Any:  # pragma: no cover - behavior is client specific
        """Persist validated records to a destination (optional)."""


@dataclass(slots=True)
class IOContext:
    """Carries user input and derived metadata for extraction."""

    requested_ids: list[Any]


@dataclass(slots=True)
class RawBatch:
    """Raw records straight from the data source."""

    requested_ids: list[Any]
    records: list[Mapping[str, Any]]
    missing_ids: set[Any] = field(default_factory=set)


@dataclass(slots=True)
class NormalizedBatch:
    """Records after normalization routines."""

    requested_ids: list[Any]
    records: list[Mapping[str, Any]]
    missing_ids: set[Any]


@dataclass(slots=True)
class ValidationError:
    """Structured validation error to keep context explicit."""

    record_id: Any
    message: str


@dataclass(slots=True)
class ValidatedBatch:
    """Result of validation stage with errors kept alongside valid rows."""

    requested_ids: list[Any]
    valid_records: list[Mapping[str, Any]]
    errors: list[ValidationError]
    missing_ids: set[Any]

    @property
    def has_errors(self) -> bool:
        return bool(self.errors)


@dataclass(slots=True)
class SaveResult:
    """Outcome of the save stage."""

    records: list[Mapping[str, Any]]
    errors: list[ValidationError]
    missing_ids: set[Any]
    output_path: Path | None = None
    saved_via_db: bool = False


class BaseChemblPipeline:
    """Generic Chembl batch pipeline template.

    Stages: io -> extract -> normalize -> validate -> save.
    """

    def __init__(
        self,
        *,
        db_client: ChemblDbClient,
        normalizer: Any,
        validator: Any,
        config: PipelineConfig,
    ) -> None:
        self.db_client = db_client
        self.normalizer = normalizer
        self.validator = validator
        self.config = config

    # --- Public API ---
    def run(self, ids: Sequence[Any]) -> SaveResult:
        """Run a full pipeline cycle for a collection of identifiers."""

        io_ctx = self._io(ids)
        raw = self._extract(io_ctx)
        normalized = self._normalize(raw)
        validated = self._validate(normalized)
        return self._save(validated)

    # --- Stage hooks ---
    def _io(self, ids: Sequence[Any]) -> IOContext:
        prepared_ids = list(dict.fromkeys(ids))
        self._log("io", {"requested": len(prepared_ids)})
        return IOContext(requested_ids=prepared_ids)

    def _extract(self, io_ctx: IOContext) -> RawBatch:
        batch_size = max(1, self.config.batch_size)
        all_records: list[Mapping[str, Any]] = []
        seen_ids: set[Any] = set()
        missing: set[Any] = set()

        for chunk in _chunked(io_ctx.requested_ids, batch_size):
            chunk_records = list(
                self.db_client.fetch_by_ids(
                    self.config.table_name,
                    self.config.id_field,
                    chunk,
                    include_related=self.config.include_related,
                )
            )
            if not chunk_records:
                missing.update(chunk)
                continue

            for record in chunk_records:
                record_id = record.get(self.config.id_field)
                if record_id is None:
                    continue
                if record_id in seen_ids:
                    continue
                seen_ids.add(record_id)
                all_records.append(record)

            found_ids = {rec.get(self.config.id_field) for rec in chunk_records if rec.get(self.config.id_field) is not None}
            missing.update(set(chunk) - found_ids)

        self._log(
            "extract",
            {"fetched": len(all_records), "missing": len(missing), "requested": len(io_ctx.requested_ids)},
        )
        return RawBatch(requested_ids=io_ctx.requested_ids, records=all_records, missing_ids=missing)

    def _normalize(self, raw: RawBatch) -> NormalizedBatch:
        normalized_records = self.normalizer.normalize_batch(raw.records)
        self._log("normalize", {"records": len(normalized_records)})
        return NormalizedBatch(
            requested_ids=raw.requested_ids,
            records=normalized_records,
            missing_ids=raw.missing_ids,
        )

    def _validate(self, normalized: NormalizedBatch) -> ValidatedBatch:
        errors = self.validator.ensure_ids_found(
            normalized.requested_ids, [rec.get(self.config.id_field) for rec in normalized.records]
        )
        more_errors, valid_records = self.validator.validate_records(normalized.records)
        errors.extend(more_errors)

        if errors and self.config.raise_on_validation_error:
            raise ValueError(f"Validation failed with {len(errors)} error(s)")

        self._log("validate", {"valid": len(valid_records), "errors": len(errors)})
        return ValidatedBatch(
            requested_ids=normalized.requested_ids,
            valid_records=valid_records,
            errors=errors,
            missing_ids=normalized.missing_ids,
        )

    def _save(self, validated: ValidatedBatch) -> SaveResult:
        if self.config.save_mode == "file":
            if not self.config.output_path:
                raise ValueError("output_path is required for save_mode='file'")
            path = Path(self.config.output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as fp:
                json.dump(validated.valid_records, fp, ensure_ascii=False, indent=2)
            self._log("save", {"mode": "file", "path": str(path)})
            return SaveResult(
                records=validated.valid_records,
                errors=validated.errors,
                missing_ids=validated.missing_ids,
                output_path=path,
            )

        if self.config.save_mode == "db":
            self.db_client.save_records(self.config.table_name, validated.valid_records)
            self._log("save", {"mode": "db", "rows": len(validated.valid_records)})
            return SaveResult(
                records=validated.valid_records,
                errors=validated.errors,
                missing_ids=validated.missing_ids,
                saved_via_db=True,
            )

        self._log("save", {"mode": "return", "rows": len(validated.valid_records)})
        return SaveResult(
            records=validated.valid_records,
            errors=validated.errors,
            missing_ids=validated.missing_ids,
        )

    # --- Helpers ---
    def _log(self, message: str, context: Mapping[str, Any]) -> None:
        if self.config.log_fn:
            try:
                self.config.log_fn(message, dict(context))
            except Exception:  # pragma: no cover - best-effort logging
                pass


class DummyChemblDbClient:
    """Simple in-memory stub useful for examples and tests."""

    def __init__(self, table_data: Mapping[Any, Mapping[str, Any]]) -> None:
        self._data = dict(table_data)
        self.saved: list[Mapping[str, Any]] = []

    def fetch_by_ids(
        self, table: str, id_field: str, ids: Sequence[Any], *, include_related: bool = False
    ) -> Sequence[Mapping[str, Any]]:
        results: list[Mapping[str, Any]] = []
        for value in ids:
            record = self._data.get(value)
            if record is None:
                continue
            payload = dict(record)
            payload[id_field] = value
            if include_related:
                payload.setdefault("related", [])
            results.append(payload)
        return results

    def save_records(self, table: str, records: Sequence[Mapping[str, Any]]) -> Any:
        self.saved.extend(records)
        return len(records)


def _chunked(items: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


__all__ = [
    "BaseChemblPipeline",
    "ChemblDbClient",
    "DummyChemblDbClient",
    "IOContext",
    "RawBatch",
    "NormalizedBatch",
    "ValidatedBatch",
    "ValidationError",
    "SaveResult",
]
