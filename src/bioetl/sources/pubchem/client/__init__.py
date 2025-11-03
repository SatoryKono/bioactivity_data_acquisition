"""Legacy PubChem client namespace."""

from bioetl.core.deprecation import warn_legacy_client

warn_legacy_client(__name__, replacement="bioetl.adapters.pubchem")

__all__: list[str] = []
