"""CLI command ``bioetl-catalog-code-symbols``."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from bioetl.cli.cli_entrypoint import TyperApp, get_typer, register_tool_app
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
catalog_code_symbols = getattr(cli_catalog_code_symbols_impl, "catalog_code_symbols")
__all__ = [* _LOGIC_EXPORTS, "catalog_code_symbols", "app", "cli_main", "run"]

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

    artifacts_path = artifacts.resolve() if artifacts else None
    try:
        result = catalog_code_symbols(artifacts_dir=artifacts_path)
    except typer.Exit:
        raise
    except (BioETLError, CircuitBreakerOpenError, HTTPError, Timeout) as exc:
        CliCommandBase.emit_error(
            template=CLI_ERROR_EXTERNAL_API,
            message=f"Catalog build failed due to external API error: {exc}",
            context={
                "command": "bioetl-catalog-code-symbols",
                "exception_type": exc.__class__.__name__,
                "artifacts": str(artifacts_path) if artifacts_path else None,
            },
            exit_code=1,
        )
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Catalog build failed: {exc}",
            context={
                "command": "bioetl-catalog-code-symbols",
                "exception_type": exc.__class__.__name__,
                "artifacts": str(artifacts_path) if artifacts_path else None,
            },
        )

    typer.echo(
        f"Catalog updated: {result.json_path.resolve()} and {result.cli_path.resolve()}"
    )
    CliCommandBase.exit(0)


app: TyperApp
run: Callable[[], None]
app, run = register_tool_app(
    name="bioetl-catalog-code-symbols",
    help_text="Build the code entity catalog and related reports",
    main_fn=cli_main,
)


if __name__ == "__main__":
    run()
