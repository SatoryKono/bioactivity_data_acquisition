"""Унифицированный раннер Typer-приложений."""

from __future__ import annotations

from bioetl.cli.tools._typer import TyperApp, run_app as _run_app

__all__ = ["run_app"]


def run_app(app: TyperApp) -> None:
    """Единая точка входа для Typer-приложений."""
    _run_app(app)


