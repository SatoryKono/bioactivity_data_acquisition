"""Public types and factories for BioETL CLI tools."""

from __future__ import annotations

from typing import NoReturn

from ._typer import TyperApp, TyperModule, create_app

__all__ = ["TyperApp", "TyperModule", "create_app", "exit_with_code"]


def exit_with_code(code: int, *, cause: Exception | None = None) -> NoReturn:
    """Thin wrapper around CliCommandBase.exit for CLI tool commands."""

    from bioetl.core.cli_base import CliCommandBase

    CliCommandBase.exit(code, cause=cause)

