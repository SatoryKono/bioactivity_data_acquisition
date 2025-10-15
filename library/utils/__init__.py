"""Compatibility wrapper for :mod:`bioactivity.utils`."""
from __future__ import annotations

from library._compat import reexport

reexport("library.utils", "bioactivity.utils", globals())
