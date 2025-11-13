"""Unified runner utilities for Typer-based applications."""

from __future__ import annotations

from bioetl.cli.tools._typer import TyperApp
from bioetl.core.cli_base import CliEntrypoint

__all__ = ["run_app"]


def run_app(app: TyperApp) -> None:
    """Delegate execution to the shared CLI entrypoint runner."""
    CliEntrypoint.run_app(app)


