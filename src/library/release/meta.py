"""Utilities for writing release metadata artefacts."""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import yaml
from jsonschema import Draft202012Validator

ISO_8601_Z_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
ALLOWED_RELEASE_SOURCES = {"cli", "status"}

META_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Bioactivity release metadata",
    "type": "object",
    "required": [
        "pipeline_version",
        "chembl_release",
        "chembl_release_source",
        "row_count",
        "checksums",
        "run_id",
        "started_at",
        "finished_at",
        "current_year",
    ],
    "properties": {
        "pipeline_version": {"type": "string", "minLength": 1},
        "chembl_release": {
            "type": "string",
            "pattern": r"^(?:ChEMBL_)?[0-9]{2}$",
        },
        "chembl_release_source": {
            "type": "string",
            "enum": sorted(ALLOWED_RELEASE_SOURCES),
        },
        "row_count": {"type": "integer", "minimum": 0},
        "checksums": {
            "type": "object",
            "additionalProperties": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
        },
        "run_id": {"type": "string", "format": "uuid"},
        "started_at": {"type": "string", "format": "date-time"},
        "finished_at": {"type": "string", "format": "date-time"},
        "current_year": {"type": "integer", "minimum": 2000},
    },
    "additionalProperties": True,
}

_validator = Draft202012Validator(META_SCHEMA)


class ReleaseMetadataError(RuntimeError):
    """Raised when release metadata cannot be produced."""

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        super().__init__(message or code)


def _ensure_iso8601_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).strftime(ISO_8601_Z_FORMAT)


def _compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_inputs(chembl_release: str) -> str:
    value = chembl_release.strip()
    if value.lower() == "unknown":
        raise ReleaseMetadataError("chembl_release_unknown", "CHEMBL_RELEASE cannot be 'unknown'")
    if not value:
        raise ReleaseMetadataError("chembl_release_unknown", "CHEMBL_RELEASE must be provided")
    return value


def write_meta(
    meta_dir: Path,
    pipeline_version: str,
    chembl_release: str,
    chembl_release_source: Literal["cli", "status"],
    data_paths: list[Path],
    row_count: int,
) -> Path:
    """Write release metadata and checksum artefacts."""

    chembl_release = _validate_inputs(chembl_release)
    if chembl_release_source not in ALLOWED_RELEASE_SOURCES:
        raise ReleaseMetadataError(
            "chembl_release_source_invalid",
            f"chembl_release_source must be one of {sorted(ALLOWED_RELEASE_SOURCES)}",
        )

    meta_dir = meta_dir.resolve()
    meta_dir.mkdir(parents=True, exist_ok=True)

    run_id = str(uuid.uuid4())
    started_at_dt = datetime.now(timezone.utc).replace(microsecond=0)

    checksums: dict[str, str] = {}
    for path in data_paths:
        resolved = Path(path).resolve()
        if not resolved.exists():
            raise FileNotFoundError(resolved)
        checksums[resolved.name] = _compute_sha256(resolved)

    finished_at_dt = datetime.now(timezone.utc).replace(microsecond=0)
    if finished_at_dt < started_at_dt:
        finished_at_dt = started_at_dt

    payload = {
        "pipeline_version": pipeline_version,
        "chembl_release": chembl_release,
        "chembl_release_source": chembl_release_source,
        "row_count": int(row_count),
        "checksums": checksums,
        "run_id": run_id,
        "started_at": _ensure_iso8601_z(started_at_dt),
        "finished_at": _ensure_iso8601_z(finished_at_dt),
        "current_year": datetime.now(timezone.utc).year,
    }

    _validator.validate(payload)

    meta_path = meta_dir / "meta.yaml"
    with meta_path.open("w", encoding="utf-8", newline="\n") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False, allow_unicode=True)

    checksum_path = meta_dir / "meta.sha256"
    checksum = _compute_sha256(meta_path)
    checksum_path.write_text(f"{checksum}\n", encoding="utf-8")

    return meta_path


__all__ = ["META_SCHEMA", "ReleaseMetadataError", "write_meta"]
