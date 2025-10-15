"""Compatibility wrapper for :mod:`bioactivity.cli`."""
from __future__ import annotations

from library._compat import reexport

reexport("library.cli", "bioactivity.cli", globals())
