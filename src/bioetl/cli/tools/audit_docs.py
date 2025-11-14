"""CLI command ``bioetl-audit-docs``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.cli.tools import emit_tool_error, exit_with_code
from bioetl.cli.tools.typer_helpers import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.core.runtime.cli_errors import CLI_ERROR_INTERNAL
from bioetl.tools.audit_docs import run_audit

typer: Any = get_typer()

__all__ = ["app", "main", "run", "run_audit"]

def main(
    artifacts: Path = typer.Option(
        Path("artifacts"),
        "--artifacts",
        help="Directory where audit reports will be written.",
        exists=False,
        file_okay=False,
        dir_okay=True,
        readable=True,
        writable=True,
    )
) -> None:
    """Run the documentation audit workflow."""

    artifacts_path = artifacts.resolve()
    try:
        run_audit(artifacts_dir=artifacts_path)
    except Exception as exc:  # noqa: BLE001
        emit_tool_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Documentation audit failed: {exc}",
            context={
                "command": "bioetl-audit-docs",
                "artifacts": str(artifacts_path),
                "exception_type": exc.__class__.__name__,
            },
            cause=exc,
        )
    typer.echo(f"Audit completed, reports stored in {artifacts_path}")
    exit_with_code(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-audit-docs",
    help_text="Run documentation audit and collect reports",
    main_fn=main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()
