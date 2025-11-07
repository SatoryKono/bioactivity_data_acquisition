"""Typer-приложения для служебных утилит BioETL."""

from __future__ import annotations

import typer

__all__ = ["create_app"]


def create_app(name: str, help_text: str) -> typer.Typer:
    """Создаёт Typer-приложение без автодополнения."""

    return typer.Typer(name=name, help=help_text, add_completion=False)

