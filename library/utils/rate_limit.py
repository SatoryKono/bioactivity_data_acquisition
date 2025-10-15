"""Shim for :mod:`bioactivity.utils.rate_limit`."""
from __future__ import annotations

from library._compat import reexport

reexport("library.utils.rate_limit", "bioactivity.utils.rate_limit", globals())
