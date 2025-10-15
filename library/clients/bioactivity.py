"""Shim for :mod:`bioactivity.clients.bioactivity`."""
from __future__ import annotations

from library._compat import reexport

reexport("library.clients.bioactivity", "bioactivity.clients.bioactivity", globals())
