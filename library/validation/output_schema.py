"""Shim for :mod:`bioactivity.schemas.output_schema`."""
from __future__ import annotations

from library._compat import reexport

reexport("library.validation.output_schema", "bioactivity.schemas.output_schema", globals())
