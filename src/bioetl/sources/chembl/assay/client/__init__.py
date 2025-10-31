"""Assay client module namespace."""

from bioetl.core.deprecation import warn_legacy_client

from .assay_client import AssayChEMBLClient

warn_legacy_client(__name__, replacement="bioetl.adapters.chembl.assay")

__all__ = ["AssayChEMBLClient"]
