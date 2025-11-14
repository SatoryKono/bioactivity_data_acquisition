"""Public types and factories for BioETL CLI tools."""

from __future__ import annotations

from typing import NoReturn

from bioetl.core.runtime.cli_errors import emit_tool_error as core_emit_tool_error

from .typer_helpers import TyperApp, TyperModule, create_app

__all__ = ["TyperApp", "TyperModule", "create_app", "exit_with_code", "emit_tool_error"]


def exit_with_code(code: int, *, cause: Exception | None = None) -> NoReturn:
    """Thin wrapper around CliCommandBase.exit for CLI tool commands."""

    from bioetl.core.runtime.cli_base import CliCommandBase

    CliCommandBase.exit(code, cause=cause)


emit_tool_error = core_emit_tool_error


