"""Shared CLI error codes and emission helpers."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, MutableMapping, NoReturn, Protocol

import typer

from bioetl.core.logging import LogEvents, UnifiedLogger

__all__ = [
    "CliErrorCode",
    "CliErrorTemplate",
    "LoggerLike",
    "CLI_ERROR_INTERNAL",
    "CLI_ERROR_CONFIG",
    "CLI_ERROR_EXTERNAL_API",
    "emit_cli_error",
    "emit_cli_error_and_exit",
    "emit_tool_error",
    "format_cli_error",
]


class CliErrorCode(str, Enum):
    """Canonical CLI error codes."""

    INTERNAL = "E001"
    CONFIG = "E002"
    EXTERNAL_API = "E003"


@dataclass(frozen=True)
class CliErrorTemplate:
    """Descriptor bundling an error code with a human-readable label."""

    code: CliErrorCode
    label: str


CLI_ERROR_INTERNAL = CliErrorTemplate(CliErrorCode.INTERNAL, "internal_error")
CLI_ERROR_CONFIG = CliErrorTemplate(CliErrorCode.CONFIG, "configuration_error")
CLI_ERROR_EXTERNAL_API = CliErrorTemplate(CliErrorCode.EXTERNAL_API, "external_api_error")

_CLI_ERROR_PREFIX = "[bioetl-cli]"


class LoggerLike(Protocol):
    """Minimal logger contract needed for CLI error emission."""

    def error(self, _event: LogEvents | str, /, **context: Any) -> Any:
        """Emit a structured error event."""
        ...


def format_cli_error(template: CliErrorTemplate, message: str) -> str:
    """Return a deterministic string representation for stderr."""

    return f"{_CLI_ERROR_PREFIX} ERROR {template.code.value}: {message}"


def emit_cli_error(
    *,
    template: CliErrorTemplate,
    message: str,
    event: LogEvents | str = LogEvents.CLI_RUN_ERROR,
    logger: LoggerLike | None = None,
    context: Mapping[str, Any] | None = None,
) -> None:
    """Emit a structured log record and deterministic stderr message."""

    log = logger or UnifiedLogger.get(__name__)
    if context:
        bound_context: MutableMapping[str, Any] = dict(context)
    else:
        bound_context = {}
    bound_context.setdefault("error_code", template.code.value)
    bound_context.setdefault("error_label", template.label)
    bound_context.setdefault("error_message", message)
    log.error(event, **bound_context)
    typer.echo(format_cli_error(template, message), err=True)


def emit_cli_error_and_exit(
    *,
    template: CliErrorTemplate,
    message: str,
    event: LogEvents | str = LogEvents.CLI_RUN_ERROR,
    logger: LoggerLike | None = None,
    context: Mapping[str, Any] | None = None,
    exit_code: int = 1,
    cause: Exception | None = None,
) -> NoReturn:
    """Emit a CLI error event and terminate execution."""

    emit_cli_error(
        template=template,
        message=message,
        event=event,
        logger=logger,
        context=context,
    )
    from bioetl.core.runtime.cli_base import CliCommandBase

    CliCommandBase.exit(exit_code, cause=cause)
    raise AssertionError("unreachable exit path")


def emit_tool_error(
    *,
    template: CliErrorTemplate,
    message: str,
    event: LogEvents | str = LogEvents.CLI_RUN_ERROR,
    logger: LoggerLike | None = None,
    context: Mapping[str, Any] | None = None,
    exit_code: int = 1,
    cause: Exception | None = None,
) -> NoReturn:
    """Emit a deterministic CLI tool error and terminate the process."""

    emit_cli_error_and_exit(
        template=template,
        message=message,
        event=event,
        logger=logger,
        context=context,
        exit_code=exit_code,
        cause=cause,
    )


