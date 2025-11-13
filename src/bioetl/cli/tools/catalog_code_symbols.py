"""CLI command ``bioetl-catalog-code-symbols``."""

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
from bioetl.tools.catalog_code_symbols import CodeCatalog
from bioetl.tools.catalog_code_symbols import (
    catalog_code_symbols as catalog_code_symbols_sync,
)

typer: Any = get_typer()

__all__ = ["app", "main", "run", "catalog_code_symbols", "CodeCatalog"]

catalog_code_symbols = catalog_code_symbols_sync

def main(
    artifacts: Path | None = typer.Option(
        None,
        "--artifacts",
        help="Directory where catalog artifacts will be stored.",
        exists=False,
        file_okay=False,
        dir_okay=True,
        writable=True,
    ),
) -> None:
    """Run the code catalog collection routine."""

    try:
        result = catalog_code_symbols(artifacts_dir=artifacts.resolve() if artifacts else None)
    except (BioETLError, CircuitBreakerOpenError, HTTPError, Timeout) as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        exit_with_code(1, cause=exc)
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        exit_with_code(1, cause=exc)

    typer.echo(
        f"Catalog updated: {result.json_path.resolve()} and {result.cli_path.resolve()}"
    )
    exit_with_code(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-catalog-code-symbols",
    help_text="Build the code entity catalog and related reports",
    main_fn=main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()

