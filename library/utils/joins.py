"""Shim for :mod:`bioactivity.utils.joins`."""
from __future__ import annotations

from library._compat import reexport

reexport("library.utils.joins", "bioactivity.utils.joins", globals())
