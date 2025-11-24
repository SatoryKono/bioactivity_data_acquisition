"""Compatibility wrapper for ChEMBL release tracking helpers.

The actual implementation now lives in :mod:`infrastructure.chembl.release_tracker`.
This module is kept only to provide the historical import path
``common.core_utils.release_tracker``.
"""

from __future__ import annotations

from infrastructure.chembl.release_tracker import (
    ChemblHandshakeResult,
    ChemblReleaseMixin,
    perform_chembl_handshake,
)

__all__ = [
    "ChemblHandshakeResult",
    "ChemblReleaseMixin",
    "perform_chembl_handshake",
]
