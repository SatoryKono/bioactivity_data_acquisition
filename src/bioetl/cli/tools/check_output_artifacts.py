"""CLI command ``bioetl-check-output-artifacts``."""

from __future__ import annotations

from typing import Any

from bioetl.cli.tools import emit_tool_error, exit_with_code
from bioetl.cli.tools.typer_helpers import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.core.runtime.cli_errors import CLI_ERROR_CONFIG, CLI_ERROR_INTERNAL
from bioetl.tools.check_output_artifacts import MAX_BYTES
from bioetl.tools.check_output_artifacts import (
    check_output_artifacts as check_output_artifacts_sync,
)

typer: Any = get_typer()

__all__ = ["app", "main", "run", "check_output_artifacts", "MAX_BYTES"]

check_output_artifacts = check_output_artifacts_sync

def main(
    max_bytes: int = typer.Option(
        MAX_BYTES,
        "--max-bytes",
        help="File size threshold (bytes) above which a file is flagged as large.",
    ),
) -> None:
    """Run the output artifact inspection."""

    try:
        errors = check_output_artifacts(max_bytes=max_bytes)
    except Exception as exc:  # noqa: BLE001
        emit_tool_error(
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
        emit_tool_error(
            template=CLI_ERROR_CONFIG,
            message=f"Found {len(errors)} output artifacts exceeding limits",
            context={
                "command": "bioetl-check-output-artifacts",
                "max_bytes": max_bytes,
                "error_count": len(errors),
            },
        )

    typer.echo("data/output directory is clean")
    exit_with_code(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-check-output-artifacts",
    help_text="Inspect the data/output directory and flag artifacts",
    main_fn=main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()

