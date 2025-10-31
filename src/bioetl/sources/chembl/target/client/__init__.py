"""Client utilities for the ChEMBL target pipeline."""

from bioetl.core.deprecation import warn_legacy_client

from .chembl_client import ClientRegistration, TargetClientManager

warn_legacy_client(__name__, replacement="bioetl.adapters.chembl.target")

__all__ = ["ClientRegistration", "TargetClientManager"]
