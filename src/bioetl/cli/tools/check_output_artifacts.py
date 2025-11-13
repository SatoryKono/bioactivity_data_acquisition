"""CLI command ``bioetl-check-output-artifacts``."""

from __future__ import annotations

from typing import Any

from bioetl.cli.tools import exit_with_code
from bioetl.cli.tools.typer_helpers import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
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
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        exit_with_code(1, cause=exc)

    if errors:
        for message in errors:
            typer.echo(message)
        exit_with_code(1)

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

