"""Activity client module namespace."""

from bioetl.core.deprecation import warn_legacy_client

from .activity_client import ActivityChEMBLClient

warn_legacy_client(__name__, replacement="bioetl.adapters.chembl.activity")

__all__ = ["ActivityChEMBLClient"]
