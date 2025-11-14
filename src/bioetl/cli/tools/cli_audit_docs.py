"""CLI command ``bioetl-audit-docs``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.cli.cli_entrypoint import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.cli.tools._logic import cli_audit_docs as cli_audit_docs_impl
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_INTERNAL

_LOGIC_EXPORTS = getattr(cli_audit_docs_impl, "__all__", [])
globals().update({symbol: getattr(cli_audit_docs_impl, symbol) for symbol in _LOGIC_EXPORTS})
__all__ = [* _LOGIC_EXPORTS, "app", "cli_main", "run"]

typer: Any = get_typer()


def cli_main(
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
    except typer.Exit:
        raise
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Documentation audit failed: {exc}",
            context={
                "command": "bioetl-audit-docs",
                "artifacts": str(artifacts_path),
                "exception_type": exc.__class__.__name__,
            },
        )
    typer.echo(f"Audit completed, reports stored in {artifacts_path}")
    CliCommandBase.exit(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-audit-docs",
    help_text="Run documentation audit and collect reports",
    main_fn=cli_main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()