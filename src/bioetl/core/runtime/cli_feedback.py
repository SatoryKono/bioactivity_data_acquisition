"""Structured helpers for deterministic CLI feedback."""

from __future__ import annotations

from typing import Any

import typer

from bioetl.core.logging import LogEvents, UnifiedLogger

__all__ = [
    "emit_section",
    "emit_line",
    "emit_kv",
    "emit_list_item",
    "emit_warning",
    "emit_error",
    "emit_success",
]

_PREFIX = "[bioetl-cli]"
_INDENT = "  "


def _format(message: str) -> str:
    return f"{_PREFIX} {message}"


def emit_section(title: str) -> None:
    """Emit a section header."""

    typer.echo(_format(title))


def emit_line(message: str, *, indent: int = 0) -> None:
    """Emit a plain informational line with optional indentation."""

    typer.echo(f"{_INDENT * indent}{message}")


def emit_kv(key: str, value: Any, *, indent: int = 0) -> None:
    """Emit a ``key: value`` line."""

    emit_line(f"{key}: {value}", indent=indent)


def emit_list_item(name: str, description: str, *, indent: int = 1) -> None:
    """Emit a formatted list entry."""

    emit_line(f"{name:<20} - {description}", indent=indent)


def emit_warning(message: str) -> None:
    """Emit a warning message with a unified prefix."""

    typer.secho(_format(f"WARN: {message}"), err=True, fg=typer.colors.YELLOW)


def emit_error(message: str) -> None:
    """Emit an error message and log it."""

    UnifiedLogger.get(__name__).error(LogEvents.CLI_RUN_ERROR, message=message)
    typer.secho(_format(f"ERROR: {message}"), err=True, fg=typer.colors.RED)


def emit_success(message: str) -> None:
    """Emit a success message."""

    typer.secho(_format(message), fg=typer.colors.GREEN)


