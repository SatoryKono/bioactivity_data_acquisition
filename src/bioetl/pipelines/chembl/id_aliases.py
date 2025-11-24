"""Alias resolvers for ChEMBL identifiers used across pipelines."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def resolve_assay_chembl_id(record: Mapping[str, Any]) -> Any:
    """Resolve assay_chembl_id from record with legacy aliases."""

    value = record.get("assay_chembl_id")
    if value is not None:
        return value
    return record.get("assay_id")


def resolve_testitem_chembl_id(record: Mapping[str, Any]) -> Any:
    """Resolve testitem_chembl_id from record with legacy aliases."""

    value = record.get("testitem_chembl_id")
    if value is None:
        value = record.get("testitem_id")
    if value is not None:
        return value
    return record.get("molecule_chembl_id")


def resolve_target_chembl_id(record: Mapping[str, Any]) -> Any:
    """Resolve target_chembl_id from record, falling back to target_id."""

    value = record.get("target_chembl_id")
    if value is not None:
        return value
    return record.get("target_id")
