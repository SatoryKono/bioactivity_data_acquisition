"""Shim for :mod:`bioactivity.utils.logging`."""
from __future__ import annotations

from library._compat import reexport

reexport("library.utils.logging", "bioactivity.utils.logging", globals())
