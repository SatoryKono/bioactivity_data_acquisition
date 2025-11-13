"""Base components for constructing the CLI layer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, ClassVar, NoReturn, TypeAlias

import typer
from structlog.stdlib import BoundLogger

from bioetl.cli.tools.typer_helpers import TyperApp
from bioetl.cli.tools.typer_helpers import run_app as typer_run_app
from bioetl.core.logging import UnifiedLogger

CommandCallable: TypeAlias = Callable[..., None]


@dataclass(frozen=True)
class CliEntrypoint:
    """Unified runner wrapper for Typer applications."""

    app: TyperApp

    def run(self) -> None:
        """Execute the configured Typer application."""

        typer_run_app(self.app)

    @staticmethod
    def run_app(app: TyperApp) -> None:
        """Execute an arbitrary Typer application without instantiating the wrapper."""

        typer_run_app(app)


class CliCommandBase:
    """Base class for CLI commands with consistent error handling."""

    exit_code_error: ClassVar[int] = 1

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

        self.emit_error("E001", f"Unhandled CLI exception: {exc}")
        self.exit(self.exit_code_error)  # pragma: no cover - process termination path

    @staticmethod
    def emit_error(code: str, message: str) -> None:
        """Emit a structured error message in a deterministic format."""

        typer.echo(f"[bioetl-cli] ERROR {code}: {message}", err=True)

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

