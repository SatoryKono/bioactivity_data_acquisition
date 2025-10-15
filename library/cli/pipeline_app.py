"""Legacy entry points for the deprecated :mod:`library.cli` package."""
from __future__ import annotations

from typing import Any

from library._compat import reexport

reexport("library.cli.pipeline_app", "bioactivity.cli", globals())


def create_pipeline_app(*_: Any, **__: Any):  # type: ignore[no-untyped-def]
    """Return the shared Typer application used by the new CLI."""

    return app


__all__.append("create_pipeline_app")
