"""Activity client module namespace."""

from bioetl.core.deprecation import warn_legacy_client

from bioetl.clients.chembl_activity import ActivityChEMBLClient

warn_legacy_client(__name__, replacement="bioetl.clients.chembl_activity")

__all__ = ["ActivityChEMBLClient"]
