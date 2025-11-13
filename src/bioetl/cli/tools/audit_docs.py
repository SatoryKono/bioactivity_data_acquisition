"""CLI command ``bioetl-audit-docs``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.cli.tools import exit_with_code
from bioetl.cli.tools.typer_helpers import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
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
    run_audit(artifacts_dir=artifacts_path)
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
