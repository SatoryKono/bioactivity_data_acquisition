"""Compatibility wrapper for :mod:`bioactivity.io_`."""
from __future__ import annotations

from library._compat import reexport

reexport("library.io", "bioactivity.io_", globals())
