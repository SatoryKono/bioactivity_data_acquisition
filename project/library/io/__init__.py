"""Proxy to :mod:`library.io`."""
from __future__ import annotations

from library._compat import reexport

reexport("project.library.io", "library.io", globals())
