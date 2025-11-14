"""CLI command ``bioetl-catalog-code-symbols``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.cli.cli_entrypoint import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.cli.tools._logic import cli_catalog_code_symbols as cli_catalog_code_symbols_impl
from bioetl.clients.client_exceptions import HTTPError, Timeout
from bioetl.core.http.api_client import CircuitBreakerOpenError
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_EXTERNAL_API, CLI_ERROR_INTERNAL
from bioetl.core.runtime.errors import BioETLError

_LOGIC_EXPORTS = getattr(cli_catalog_code_symbols_impl, "__all__", [])
globals().update(
    {symbol: getattr(cli_catalog_code_symbols_impl, symbol) for symbol in _LOGIC_EXPORTS}
)
__all__ = [* _LOGIC_EXPORTS, "app", "cli_main", "run"]

typer: Any = get_typer()


def cli_main(
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
        result = cli_catalog_code_symbols_impl.catalog_code_symbols(
            artifacts_dir=artifacts.resolve() if artifacts else None
        )
    except (BioETLError, CircuitBreakerOpenError, HTTPError, Timeout) as exc:
        CliCommandBase.emit_error(
            template=CLI_ERROR_EXTERNAL_API,
            message=f"Catalog build failed due to external API error: {exc}",
            context={
                "command": "bioetl-catalog-code-symbols",
                "exception_type": exc.__class__.__name__,
                "artifacts": str(artifacts.resolve()) if artifacts else None,
            },
            exit_code=3,
            cause=exc,
        )
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Catalog build failed: {exc}",
            context={
                "command": "bioetl-catalog-code-symbols",
                "exception_type": exc.__class__.__name__,
                "artifacts": str(artifacts.resolve()) if artifacts else None,
            },
            cause=exc,
        )

    typer.echo(
        f"Catalog updated: {result.json_path.resolve()} and {result.cli_path.resolve()}"
    )
    CliCommandBase.exit(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-catalog-code-symbols",
    help_text="Build the code entity catalog and related reports",
    main_fn=cli_main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()