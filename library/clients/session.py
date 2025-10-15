"""Shim for :mod:`bioactivity.clients.session`."""
from __future__ import annotations

from library._compat import reexport

reexport("library.clients.session", "bioactivity.clients.session", globals())
