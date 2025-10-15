"""Shim for :mod:`bioactivity.clients.base`."""
from __future__ import annotations

from library._compat import reexport

reexport("library.clients.base", "bioactivity.clients.base", globals())
