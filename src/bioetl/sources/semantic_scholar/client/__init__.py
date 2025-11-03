"""Semantic Scholar client components."""

from bioetl.core.deprecation import warn_legacy_client

warn_legacy_client(__name__, replacement="bioetl.adapters.semantic_scholar")

__all__: list[str] = []
