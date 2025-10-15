"""Shim for :mod:`bioactivity.io_.read_write`."""
from __future__ import annotations

from library._compat import reexport

reexport("library.io.read_write", "bioactivity.io_.read_write", globals())
