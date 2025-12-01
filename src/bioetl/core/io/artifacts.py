from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, MutableMapping, Sequence

import pandera.pandas as pa

HASH_ALGORITHM = "sha256"


def _ensure_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    return str(value).encode("utf-8")


def compute_file_hash(path: Path, *, chunk_size: int = 8192) -> str:
    digest = hashlib.new(HASH_ALGORITHM)
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def hash_row(values: Sequence[Any]) -> str:
    digest = hashlib.new(HASH_ALGORITHM)
    for value in values:
        digest.update(_ensure_bytes(value))
    return digest.hexdigest()


def hash_business_key(values: Sequence[Any]) -> str:
    return hash_row(values)


@dataclass(slots=True)
class DeterminismSettings:
    sort_by: tuple[str, ...] | None = None


@dataclass(slots=True)
class SchemaRegistryEntry:
    identifier: str
    schema: pa.DataFrameSchema
    version: str
    column_order: tuple[str, ...] = ()
    determinism: DeterminismSettings | None = None
    business_key_fields: tuple[str, ...] = ()
    row_hash_fields: tuple[str, ...] = ()
    required_fields: tuple[str, ...] = ()

    def with_determinism(
        self,
        sort_by: Sequence[str] | None,
    ) -> "SchemaRegistryEntry":
        settings = DeterminismSettings(
            sort_by=tuple(sort_by) if sort_by else None
        )
        return SchemaRegistryEntry(
            identifier=self.identifier,
            schema=self.schema,
            version=self.version,
            column_order=self.column_order,
            determinism=settings,
            business_key_fields=self.business_key_fields,
            row_hash_fields=self.row_hash_fields,
            required_fields=self.required_fields,
        )


class SchemaRegistry:
    """Простой in-memory реестр схем Pandera."""

    def __init__(self) -> None:
        self._entries: MutableMapping[str, SchemaRegistryEntry] = {}

    def register(self, entry: SchemaRegistryEntry) -> None:
        if entry.identifier in self._entries:
            msg = f"Schema '{entry.identifier}' is already registered"
            raise ValueError(msg)
        missing = [
            col
            for col in entry.column_order
            if col not in entry.schema.columns
        ]
        if missing:
            msg = (
                "Schema '"
                f"{entry.identifier}"
                "' column_order references missing columns: "
                f"{missing}"
            )
            raise ValueError(msg)
        self._entries[entry.identifier] = entry

    def get(self, identifier: str) -> SchemaRegistryEntry:
        try:
            return self._entries[identifier]
        except KeyError as exc:  # pragma: no cover - defensive
            msg = f"Schema '{identifier}' is not registered"
            raise KeyError(msg) from exc

    def as_mapping(self) -> Mapping[str, SchemaRegistryEntry]:
        return dict(self._entries)


@dataclass(slots=True)
class WriteArtifacts:
    dataset: str | None = None
    data_path: Path | None = None
    meta_path: Path | None = None
    manifest_path: Path | None = None
    quality_report_path: Path | None = None
    qc_summary_path: Path | None = None
    extra: dict[str, Path] = field(default_factory=dict)


@dataclass(slots=True)
class RunArtifacts:
    output_dir: Path
    logs_directory: Path
    write_artifacts: WriteArtifacts | None = None
    qc_metrics_path: Path | None = None


__all__ = [
    "DeterminismSettings",
    "RunArtifacts",
    "SchemaRegistry",
    "SchemaRegistryEntry",
    "WriteArtifacts",
    "compute_file_hash",
    "hash_business_key",
    "hash_row",
]
