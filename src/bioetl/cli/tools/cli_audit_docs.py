"""CLI command ``bioetl-audit-docs``."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from bioetl.cli.cli_entrypoint import TyperApp, get_typer, register_tool_app
from bioetl.cli.tools._logic.cli_audit_docs import (
    audit_broken_links,
    extract_pipeline_info,
    find_lychee_missing,
    run_audit,
)
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_INTERNAL

__all__ = [
    "audit_broken_links",
    "find_lychee_missing",
    "extract_pipeline_info",
    "run_audit",
    "app",
    "cli_main",
    "run",
]

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


app: TyperApp
run: Callable[[], None]
app, run = register_tool_app(
    name="bioetl-audit-docs",
    help_text="Run documentation audit and collect reports",
    main_fn=cli_main,
)


if __name__ == "__main__":
    run()
