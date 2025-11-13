"""CLI command ``bioetl-vocab-audit``."""

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
from bioetl.tools.vocab_audit import (
    DEFAULT_META,
    DEFAULT_OUTPUT,
    VocabAuditResult,
)
from bioetl.tools.vocab_audit import (
    audit_vocabularies as audit_vocabularies_sync,
)

typer: Any = get_typer()

__all__ = [
    "app",
    "main",
    "run",
    "audit_vocabularies",
    "DEFAULT_OUTPUT",
    "DEFAULT_META",
    "VocabAuditResult",
]

audit_vocabularies = audit_vocabularies_sync


def main(
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
        result = audit_vocabularies(
            store=store.resolve() if store else None,
            output=output,
            meta=meta,
            pages=pages,
            page_size=page_size,
        )
    except (BioETLError, CircuitBreakerOpenError, HTTPError, Timeout) as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        exit_with_code(1, cause=exc)
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        exit_with_code(1, cause=exc)

    typer.echo(
        "Vocabulary audit completed: "
        f"{len(result.rows)} records, CSV {result.output.resolve()}, meta {result.meta.resolve()}"
    )
    exit_with_code(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-vocab-audit",
    help_text="Audit ChEMBL vocabularies and generate a report",
    main_fn=main,
)


def run() -> None:
    """Execute the Typer application."""
    run_app(app)


if __name__ == "__main__":
    run()

