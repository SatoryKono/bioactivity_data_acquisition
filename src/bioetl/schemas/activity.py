"""Backward-compatible import facade for the ChEMBL activity schema."""

from __future__ import annotations

from bioetl.schemas.chembl_activity import *  # noqa: F401,F403

__all__ = [name for name in globals() if not name.startswith("_")]
