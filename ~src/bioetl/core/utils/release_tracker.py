"""Compatibility wrapper for ChEMBL release tracking helpers.

The actual implementation now lives in :mod:`bioetl.chembl.common.release_tracker`.
This module is kept only to provide the historical import path
``bioetl.core.utils.release_tracker``.
"""

from __future__ import annotations

from bioetl.chembl.common.release_tracker import (
    ChemblHandshakeResult,
    ChemblReleaseMixin,
    perform_chembl_handshake,
)

__all__ = [
    "ChemblHandshakeResult",
    "ChemblReleaseMixin",
    "perform_chembl_handshake",
]
