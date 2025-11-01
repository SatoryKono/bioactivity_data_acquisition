"""Golden regression tests for UnifiedSchema metadata."""

from __future__ import annotations

import hashlib
from pathlib import Path

import yaml

from bioetl.core.unified_schema import get_schema_metadata

_ENTITIES = ("document", "assay", "activity", "target", "testitem")
_GOLDEN_PATH = Path(__file__).with_name("data") / "unified_schema_metadata.yaml"


def _hash_columns(columns: list[str]) -> str:
    digest = hashlib.sha256()
    digest.update("\n".join(columns).encode("utf-8"))
    return digest.hexdigest()


def test_unified_schema_metadata_matches_golden() -> None:
    actual: dict[str, dict[str, object]] = {}

    for entity in _ENTITIES:
        metadata = get_schema_metadata(entity)
        assert metadata is not None
        columns = list(metadata.schema.get_column_order())
        actual[entity] = {
            "schema_id": metadata.schema_id,
            "schema_version": metadata.version,
            "column_count": len(columns),
            "column_order_hash": _hash_columns(columns),
        }

    with _GOLDEN_PATH.open("r", encoding="utf-8") as handle:
        expected = yaml.safe_load(handle) or {}

    assert actual == expected
