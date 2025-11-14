"""CLI command ``bioetl-vocab-audit``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.cli.cli_entrypoint import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.cli.tools._logic import cli_vocab_audit as cli_vocab_audit_impl
from bioetl.clients.client_exceptions import HTTPError, Timeout
from bioetl.core.http.api_client import CircuitBreakerOpenError
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_EXTERNAL_API, CLI_ERROR_INTERNAL
from bioetl.core.runtime.errors import BioETLError

_LOGIC_EXPORTS = getattr(cli_vocab_audit_impl, "__all__", [])
globals().update({symbol: getattr(cli_vocab_audit_impl, symbol) for symbol in _LOGIC_EXPORTS})
__all__ = [* _LOGIC_EXPORTS, "app", "cli_main", "run"]

typer: Any = get_typer()
DEFAULT_OUTPUT = cli_vocab_audit_impl.DEFAULT_OUTPUT
DEFAULT_META = cli_vocab_audit_impl.DEFAULT_META


def cli_main(
    store: Path | None = typer.Option(
        None,
        "--store",
        help="Directory or YAML file containing ChEMBL vocabularies.",
        exists=True,
        file_okay=True,
        dir_okay=True,
        readable=True,
    ),
    output: Path = typer.Option(
        DEFAULT_OUTPUT,
        "--output",
        help="Path to the CSV report with vocabulary audit results.",
        exists=False,
        file_okay=True,
        dir_okay=False,
        writable=True,
    ),
    meta: Path = typer.Option(
        DEFAULT_META,
        "--meta",
        help="Path to the audit meta.yaml file.",
        exists=False,
        file_okay=True,
        dir_okay=False,
        writable=True,
    ),
    pages: int = typer.Option(
        10,
        "--pages",
        help="Number of ChEMBL API pages to sample values from.",
    ),
    page_size: int = typer.Option(
        1000,
        "--page-size",
        help="Page size to use for ChEMBL API pagination.",
    ),
) -> None:
    """Run the vocabulary audit pipeline."""

    try:
        result = cli_vocab_audit_impl.audit_vocabularies(
            store=store.resolve() if store else None,
            output=output,
            meta=meta,
            pages=pages,
            page_size=page_size,
        )
    except (BioETLError, CircuitBreakerOpenError, HTTPError, Timeout) as exc:
        CliCommandBase.emit_error(
            template=CLI_ERROR_EXTERNAL_API,
            message=f"Vocabulary audit failed due to external API error: {exc}",
            context={
                "command": "bioetl-vocab-audit",
                "exception_type": exc.__class__.__name__,
                "store": str(store.resolve()) if store else None,
                "output": str(output.resolve()) if hasattr(output, "resolve") else str(output),
                "meta": str(meta.resolve()) if hasattr(meta, "resolve") else str(meta),
                "pages": pages,
                "page_size": page_size,
            },
            exit_code=3,
            cause=exc,
        )
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Vocabulary audit failed: {exc}",
            context={
                "command": "bioetl-vocab-audit",
                "exception_type": exc.__class__.__name__,
                "store": str(store.resolve()) if store else None,
                "output": str(output.resolve()) if hasattr(output, "resolve") else str(output),
                "meta": str(meta.resolve()) if hasattr(meta, "resolve") else str(meta),
                "pages": pages,
                "page_size": page_size,
            },
            cause=exc,
        )

    typer.echo(
        "Vocabulary audit completed: "
        f"{len(result.rows)} records, CSV {result.output.resolve()}, meta {result.meta.resolve()}"
    )
    CliCommandBase.exit(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-vocab-audit",
    help_text="Audit ChEMBL vocabularies and generate a report",
    main_fn=cli_main,
)


def run() -> None:
    """Execute the Typer application."""
    run_app(app)


if __name__ == "__main__":
    run()