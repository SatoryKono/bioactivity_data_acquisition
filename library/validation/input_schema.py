"""Shim for :mod:`bioactivity.schemas.input_schema`."""
from __future__ import annotations

from library._compat import reexport

reexport("library.validation.input_schema", "bioactivity.schemas.input_schema", globals())
