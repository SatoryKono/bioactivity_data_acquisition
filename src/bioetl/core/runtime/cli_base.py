"""Base components for constructing the CLI layer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, ClassVar, Mapping, NoReturn, TypeAlias

import typer
from structlog.stdlib import BoundLogger

from bioetl.cli.cli_entrypoint import TyperApp
from bioetl.cli.cli_entrypoint import run_app as typer_run_app
from bioetl.core.logging import LogEvents, UnifiedLogger
from bioetl.core.runtime.cli_errors import (
    CLI_ERROR_INTERNAL,
    CliErrorTemplate,
    LoggerLike,
    emit_cli_error_and_exit,
)

CommandCallable: TypeAlias = Callable[..., None]


def run_typer_app(
    app: TyperApp,
    *,
    preconfigured_logger: BoundLogger | None = None,
) -> None:
    """Execute a Typer application with unified error handling."""

    logger = preconfigured_logger or UnifiedLogger.get(__name__)
    try:
        typer_run_app(app)
    except typer.Exit:
        raise
    except typer.BadParameter:
        raise
    except Exception as exc:  # noqa: BLE001 - delegated structured handling
        emit_cli_error_and_exit(
            template=CLI_ERROR_INTERNAL,
            message=f"Typer application failed: {exc}",
            logger=logger,
            event=LogEvents.CLI_RUN_ERROR,
            context={
                "exception_type": exc.__class__.__name__,
                "exc_info": True,
            },
            exit_code=CliCommandBase.exit_code_error,
            cause=exc,
        )


@dataclass(frozen=True)
class CliEntrypoint:
    """Unified runner wrapper for Typer applications."""

    app: TyperApp

    def run(self) -> None:
        """Execute the configured Typer application."""

        run_typer_app(self.app)

    @staticmethod
    def run_app(app: TyperApp) -> None:
        """Execute an arbitrary Typer application without instantiating the wrapper."""

        run_typer_app(app)


class CliCommandBase:
    """Base class for CLI commands with consistent error handling."""

    exit_code_error: ClassVar[int] = 1
    error_template: ClassVar[CliErrorTemplate] = CLI_ERROR_INTERNAL

    def __init__(self, *, logger: BoundLogger | None = None) -> None:
        """Initialize the command base with a bound logger."""
        self._logger = logger or UnifiedLogger.get(__name__)

    @property
    def logger(self) -> BoundLogger:
        """Return the bound logger for the command."""

        return self._logger

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        """Invoke the command runner with the provided arguments."""
        self.invoke(*args, **kwargs)

    def invoke(self, *args: Any, **kwargs: Any) -> None:
        """Execute the command with unified error handling."""

        try:
            self.handle(*args, **kwargs)
        except typer.Exit:
            raise
        except typer.BadParameter:
            raise
        except Exception as exc:  # noqa: BLE001 - manual handling below
            self.handle_exception(exc)

    def handle(self, *args: Any, **kwargs: Any) -> None:
        """Implement the command logic; must be overridden in subclasses."""

        raise NotImplementedError

    def handle_exception(self, exc: Exception) -> NoReturn:
        """Handle unexpected exceptions and terminate the process."""

        self.emit_error(
            template=self.error_template,
            message=f"Unhandled CLI exception: {exc}",
            logger=self.logger,
            event=LogEvents.CLI_RUN_ERROR,
            context={
                "exception_type": exc.__class__.__name__,
                "exc_info": True,
            },
            exit_code=self.exit_code_error,
            cause=exc,
        )

    @staticmethod
    def emit_error(
        *,
        template: CliErrorTemplate,
        message: str,
        logger: LoggerLike | None = None,
        event: LogEvents | str = LogEvents.CLI_RUN_ERROR,
        context: Mapping[str, Any] | None = None,
        exit_code: int | None = None,
        cause: Exception | None = None,
    ) -> NoReturn:
        """Emit a structured error message and terminate the command."""

        emit_cli_error_and_exit(
            template=template,
            message=message,
            event=event,
            logger=logger,
            context=context,
            exit_code=exit_code or CliCommandBase.exit_code_error,
            cause=cause,
        )

    @staticmethod
    def exit(code: int, *, cause: Exception | None = None) -> NoReturn:
        """Terminate the command with the provided exit code."""

        exit_exc = typer.Exit(code=code)
        setattr(exit_exc, "code", code)
        if cause is not None:
            raise exit_exc from cause
        raise exit_exc

    @classmethod
    def build(cls, *init_args: Any, **init_kwargs: Any) -> CommandCallable:
        """Create a Typer-compatible callable adapter."""

        def _command(*args: Any, **kwargs: Any) -> None:
            """Instantiate the command and delegate execution."""
            runner = cls(*init_args, **init_kwargs)
            runner.invoke(*args, **kwargs)

        return _command

