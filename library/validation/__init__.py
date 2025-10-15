"""Compatibility wrapper for schema modules."""
from __future__ import annotations

from library._compat import reexport

reexport("library.validation", "bioactivity.schemas", globals())
