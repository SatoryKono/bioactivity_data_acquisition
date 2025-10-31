"""Crossref client components."""

from bioetl.core.deprecation import warn_legacy_client

warn_legacy_client(__name__, replacement="bioetl.adapters.crossref")

__all__ = []
