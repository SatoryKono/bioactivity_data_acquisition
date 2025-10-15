"""Shim for :mod:`bioactivity.clients.semantic_scholar`."""
from __future__ import annotations

from library._compat import reexport

reexport(
    "library.clients.semscholar",
    "bioactivity.clients.semantic_scholar",
    globals(),
    aliases={"SemanticScholarClient": "SemanticScholarClient"},
)
