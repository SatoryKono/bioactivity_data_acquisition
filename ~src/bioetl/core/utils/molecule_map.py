"""Join helpers for linking activity records to molecule metadata.

This module is kept as a thin compatibility wrapper. The implementation of
``join_activity_with_molecule`` now lives in :mod:`bioetl.chembl.common`.
"""

from __future__ import annotations

from bioetl.chembl.common import join_activity_with_molecule

__all__ = ["join_activity_with_molecule"]
