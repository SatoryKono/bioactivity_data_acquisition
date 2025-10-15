"""Shim for :mod:`bioactivity.clients.crossref`."""
from __future__ import annotations

from library._compat import reexport

reexport("library.clients.crossref", "bioactivity.clients.crossref", globals())
