"""Deprecated compatibility layer for ChEMBL client factories.

This module preserves historical import paths by re-exporting factories from
``bioetl.clients.factories`` while emitting a :class:`DeprecationWarning`.
"""
from __future__ import annotations

import warnings

from bioetl.clients.factories import (
    default_activity_client_factory,
    default_chembl_factory,
    make_chembl_client,
)

warnings.warn(
    "Importing from 'bioetl.clients.chembl.factories' is deprecated; "
    "use 'bioetl.clients.factories' instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "default_activity_client_factory",
    "default_chembl_factory",
    "make_chembl_client",
]

