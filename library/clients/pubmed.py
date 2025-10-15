"""Shim for :mod:`bioactivity.clients.pubmed`."""
from __future__ import annotations

from library._compat import reexport

reexport(
    "library.clients.pubmed",
    "bioactivity.clients.pubmed",
    globals(),
    aliases={"PubmedClient": "PubMedClient"},
)
