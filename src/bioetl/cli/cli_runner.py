"""Унифицированный раннер Typer-приложений."""

from __future__ import annotations

from bioetl.cli.tools._typer import TyperApp
from bioetl.core.cli_base import CliEntrypoint

__all__ = ["run_app"]


def run_app(app: TyperApp) -> None:
    """Единая точка входа для Typer-приложений."""
    CliEntrypoint.run_app(app)


