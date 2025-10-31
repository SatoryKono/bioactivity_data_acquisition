"""TestItem client module namespace."""

from bioetl.core.deprecation import warn_legacy_client

from .client import TestItemChEMBLClient

warn_legacy_client(__name__, replacement="bioetl.adapters.chembl.testitem")

__all__ = ["TestItemChEMBLClient"]
