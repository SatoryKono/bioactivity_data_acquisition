"""Унифицированный раннер Typer-приложений."""

from __future__ import annotations

from typing import Callable


def run_app(app: Callable[[], None]) -> None:
    """Единая точка входа для Typer-приложений."""
    app()


