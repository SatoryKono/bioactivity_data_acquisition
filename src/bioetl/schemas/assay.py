"""Backward-compatible import facade for the ChEMBL assay schema."""

from __future__ import annotations

from bioetl.schemas.chembl_assay import *  # noqa: F401,F403

__all__ = [name for name in globals() if not name.startswith("_")]
