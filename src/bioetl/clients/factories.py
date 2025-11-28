"""Deprecated compatibility layer for ChEMBL client factories."""
from __future__ import annotations

import warnings

from bioetl.clients.chembl import factories as _chembl_factories

warnings.warn(
    "'bioetl.clients.factories' is deprecated; use "
    "'bioetl.clients.chembl.factories' instead.",
    DeprecationWarning,
    stacklevel=2,
)

default_chembl_factory = _chembl_factories.default_chembl_factory
make_chembl_client = _chembl_factories.make_chembl_client
default_activity_client_factory = _chembl_factories.default_activity_client_factory

__all__ = [
    "default_chembl_factory",
    "make_chembl_client",
    "default_activity_client_factory",
]
