"""Shim for :mod:`bioactivity.clients.openalex`."""
from __future__ import annotations

from library._compat import reexport

reexport("library.clients.openalex", "bioactivity.clients.openalex", globals())
