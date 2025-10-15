"""Proxy to :mod:`library.utils`."""
from __future__ import annotations

from library._compat import reexport

reexport("project.library.utils", "library.utils", globals())
