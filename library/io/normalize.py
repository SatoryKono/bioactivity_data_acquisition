"""Shim for :mod:`bioactivity.io_.normalize`."""
from __future__ import annotations

from library._compat import reexport

reexport("library.io.normalize", "bioactivity.io_.normalize", globals())
