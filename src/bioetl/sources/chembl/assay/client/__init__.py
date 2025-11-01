"""Assay client module namespace."""

from bioetl.core.deprecation import warn_legacy_client

from bioetl.clients.chembl_assay import AssayChEMBLClient

warn_legacy_client(__name__, replacement="bioetl.clients.chembl_assay")

__all__ = ["AssayChEMBLClient"]
