"""Compatibility shim exporting the target schema under the legacy namespace."""

from __future__ import annotations

from bioetl.schemas.chembl_target_schema import COLUMN_ORDER, SCHEMA_VERSION, TargetSchema

__all__ = ["SCHEMA_VERSION", "COLUMN_ORDER", "TargetSchema"]


