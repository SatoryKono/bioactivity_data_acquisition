"""CLI command ``bioetl-create-matrix-doc-code``."""

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
from bioetl.clients.client_exceptions import HTTPError, Timeout
from bioetl.core.http.api_client import CircuitBreakerOpenError
from bioetl.core.runtime.errors import BioETLError
from bioetl.tools.create_matrix_doc_code import DocCodeMatrix, build_matrix
from bioetl.tools.create_matrix_doc_code import (
    write_matrix as write_matrix_sync,
)

typer: Any = get_typer()

__all__ = ["app", "main", "run", "write_matrix", "build_matrix", "DocCodeMatrix"]

write_matrix = write_matrix_sync

def main(
    artifacts: Path = typer.Option(
        Path("artifacts"),
        "--artifacts",
        help="Directory for storing the Doc↔Code matrix artifacts.",
        exists=False,
        file_okay=False,
        dir_okay=True,
        writable=True,
    ),
) -> None:
    """Produce the documentation-to-code correspondence matrix."""

    try:
        result = write_matrix(artifacts_dir=artifacts.resolve())
    except (BioETLError, CircuitBreakerOpenError, HTTPError, Timeout) as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        exit_with_code(1, cause=exc)
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        exit_with_code(1, cause=exc)

    typer.echo(
        f"Matrix with {len(result.rows)} rows saved to "
        f"{result.csv_path.resolve()} and {result.json_path.resolve()}"
    )
    exit_with_code(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-create-matrix-doc-code",
    help_text="Generate the Doc↔Code matrix and export artifacts",
    main_fn=main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()

