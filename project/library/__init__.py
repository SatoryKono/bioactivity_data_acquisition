"""Compatibility entry point mirroring :mod:`library`."""
from __future__ import annotations

from library._compat import reexport

reexport("project.library", "library", globals())
