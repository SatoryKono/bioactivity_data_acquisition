"""Typer-приложения для служебных утилит BioETL."""

from __future__ import annotations

import typer

from bioetl.cli.runner import runner_factory

__all__ = ["create_app", "run_app", "runner_factory"]


def create_app(name: str, help_text: str) -> typer.Typer:
    """Создаёт Typer-приложение без автодополнения."""

    return typer.Typer(name=name, help=help_text, add_completion=False)


def run_app(app: typer.Typer, *, setup_logging: bool = True) -> None:
    """Запускает Typer-приложение через общий раннер."""

    runner = runner_factory(app, setup_logging=setup_logging)
    runner()
