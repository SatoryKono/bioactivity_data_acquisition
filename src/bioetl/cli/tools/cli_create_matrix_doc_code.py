"""CLI command ``bioetl-create-matrix-doc-code``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.cli.cli_entrypoint import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.cli.tools._logic import cli_create_matrix_doc_code as cli_create_matrix_doc_code_impl
from bioetl.clients.client_exceptions import HTTPError, Timeout
from bioetl.core.http.api_client import CircuitBreakerOpenError
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_EXTERNAL_API, CLI_ERROR_INTERNAL
from bioetl.core.runtime.errors import BioETLError

_LOGIC_EXPORTS = getattr(cli_create_matrix_doc_code_impl, "__all__", [])
globals().update(
    {symbol: getattr(cli_create_matrix_doc_code_impl, symbol) for symbol in _LOGIC_EXPORTS}
)
__all__ = [* _LOGIC_EXPORTS, "app", "cli_main", "run"]

typer: Any = get_typer()


def cli_main(
    artifacts: Path = typer.Option(
        Path("artifacts"),
        "--artifacts",
        help="Directory for storing the Doc<->Code matrix artifacts.",
        exists=False,
        file_okay=False,
        dir_okay=True,
        writable=True,
    ),
) -> None:
    """Produce the documentation-to-code correspondence matrix."""

    artifacts_path = artifacts.resolve()
    try:
        result = cli_create_matrix_doc_code_impl.write_matrix(artifacts_dir=artifacts_path)
    except (BioETLError, CircuitBreakerOpenError, HTTPError, Timeout) as exc:
        CliCommandBase.emit_error(
            template=CLI_ERROR_EXTERNAL_API,
            message=f"Doc<->Code matrix build failed due to external API error: {exc}",
            context={
                "command": "bioetl-create-matrix-doc-code",
                "artifacts": str(artifacts_path),
                "exception_type": exc.__class__.__name__,
            },
            exit_code=3,
            cause=exc,
        )
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Doc<->Code matrix build failed: {exc}",
            context={
                "command": "bioetl-create-matrix-doc-code",
                "artifacts": str(artifacts_path),
                "exception_type": exc.__class__.__name__,
            },
            cause=exc,
        )

    typer.echo(
        f"Matrix with {len(result.rows)} rows saved to "
        f"{result.csv_path.resolve()} and {result.json_path.resolve()}"
    )
    CliCommandBase.exit(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-create-matrix-doc-code",
    help_text="Generate the Doc<->Code matrix and export artifacts",
    main_fn=cli_main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()