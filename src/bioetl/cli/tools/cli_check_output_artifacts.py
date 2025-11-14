"""CLI command ``bioetl-check-output-artifacts``."""

from __future__ import annotations

from typing import Any, Callable

from bioetl.cli.cli_entrypoint import TyperApp, get_typer, register_tool_app
from bioetl.cli.tools._logic import cli_check_output_artifacts as cli_check_output_artifacts_impl
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_CONFIG, CLI_ERROR_INTERNAL

_LOGIC_EXPORTS = getattr(cli_check_output_artifacts_impl, "__all__", [])
globals().update(
    {symbol: getattr(cli_check_output_artifacts_impl, symbol) for symbol in _LOGIC_EXPORTS}
)
check_output_artifacts = getattr(cli_check_output_artifacts_impl, "check_output_artifacts")
__all__ = [
    * _LOGIC_EXPORTS,
    "check_output_artifacts",
    "app",
    "cli_main",
    "run",
]  # pyright: ignore[reportUnsupportedDunderAll]

typer: Any = get_typer()
MAX_BYTES = cli_check_output_artifacts_impl.MAX_BYTES


def cli_main(
    max_bytes: int = typer.Option(
        MAX_BYTES,
        "--max-bytes",
        help="File size threshold (bytes) above which a file is flagged as large.",
    ),
) -> None:
    """Run the output artifact inspection."""

    errors: list[str]

    try:
        errors = check_output_artifacts(max_bytes=max_bytes)
    except typer.Exit:
        raise
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Output artifact inspection failed: {exc}",
            context={
                "command": "bioetl-check-output-artifacts",
                "max_bytes": max_bytes,
                "exception_type": exc.__class__.__name__,
            },
            cause=exc,
        )

    if errors:
        for message in errors:
            typer.echo(message)
        CliCommandBase.emit_error(
            template=CLI_ERROR_CONFIG,
            message=f"Found {len(errors)} output artifacts exceeding limits",
            context={
                "command": "bioetl-check-output-artifacts",
                "max_bytes": max_bytes,
                "error_count": len(errors),
            },
            exit_code=1,
        )

    typer.echo("data/output directory is clean")
    CliCommandBase.exit(0)


app: TyperApp
run: Callable[[], None]
app, run = register_tool_app(
    name="bioetl-check-output-artifacts",
    help_text="Inspect the data/output directory and flag artifacts",
    main_fn=cli_main,
)


if __name__ == "__main__":
    run()
